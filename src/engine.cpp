// Shopping Sessions v2 -- native, exact, sharded temporal reduction.
// All IDs stay opaque. Hash collisions are resolved by comparing their bytes.
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using SV=std::string_view;
namespace fs=std::filesystem;
constexpr unsigned SHARDS=32;
constexpr int64_t DAY=86400000, WEEK=7*DAY;
constexpr int64_t BAD=std::numeric_limits<int64_t>::min();
static int workers=8;
static auto boot=std::chrono::steady_clock::now();
static void log(const char* s) { std::cerr<<"["<<std::chrono::duration<double>(std::chrono::steady_clock::now()-boot).count()<<"s] "<<s<<'\n'; }
static void check(const arrow::Status& s) { if(!s.ok())throw std::runtime_error(s.ToString()); }
template<class T> T take(arrow::Result<T> r){if(!r.ok())throw std::runtime_error(r.status().ToString());return std::move(r).ValueUnsafe();}
static bool same(SV a,SV b){return a.data()==b.data()&&a.size()==b.size() ? true : a==b;}
static uint64_t mix(uint64_t x){x^=x>>30;x*=0xbf58476d1ce4e5b9ULL;x^=x>>27;x*=0x94d049bb133111ebULL;return x^(x>>31);}
static uint64_t hashstr(SV s){
    const char* p=s.data();size_t n=s.size();uint64_t h=0x9e3779b97f4a7c15ULL^n;
    while(n>=8){uint64_t a;memcpy(&a,p,8);h=mix(h^a);p+=8;n-=8;}
    uint64_t tail=0;if(n)memcpy(&tail,p,n);return mix(h^tail);
}
// Compact linear-probing map. Empty string keys are never inserted.
template<class V> class FlatMap {
    struct Slot{SV key{};V value{};};
    std::vector<Slot> slots;size_t used=0;
    void grow(size_t n){auto old=std::move(slots);slots.resize(n);used=0;for(auto& x:old)if(x.key.data())insert(x.key,x.value);}
public:
    explicit FlatMap(size_t n=16){size_t cap=16;while(cap<n*2)cap*=2;slots.resize(cap);}
    V* find(SV key){return const_cast<V*>(static_cast<const FlatMap*>(this)->find(key));}
    const V* find(SV key)const{size_t i=hashstr(key)&(slots.size()-1);while(slots[i].key.data()){if(same(slots[i].key,key))return &slots[i].value;i=(i+1)&(slots.size()-1);}return nullptr;}
    std::pair<V*,bool> insert(SV key,V value){
        if((used+1)*10>slots.size()*7)grow(slots.size()*2);
        size_t i=hashstr(key)&(slots.size()-1);
        while(slots[i].key.data()){if(same(slots[i].key,key))return {&slots[i].value,false};i=(i+1)&(slots.size()-1);}
        slots[i]={key,value};++used;return {&slots[i].value,true};
    }
    template<class F>void each(F f){for(auto& s:slots)if(s.key.data())f(s.key,s.value);}
    size_t size()const{return used;}
};
struct Mapped {
    int fd=-1;const char* data=nullptr;size_t size=0;
    explicit Mapped(const fs::path& p){
        fd=open(p.c_str(),O_RDONLY);if(fd<0)throw std::runtime_error("Cannot open "+p.string());
        struct stat st{};if(fstat(fd,&st))throw std::runtime_error("Cannot stat input");size=st.st_size;
        if(size){auto q=mmap(nullptr,size,PROT_READ,MAP_PRIVATE,fd,0);if(q==MAP_FAILED)throw std::runtime_error("mmap failed");data=(const char*)q;}
    }
    ~Mapped(){if(data)munmap((void*)data,size);if(fd>=0)close(fd);}
};
struct Task{const char* begin;const char* end;};
struct Input {
    std::vector<std::unique_ptr<Mapped>> maps;
    std::vector<Task> tasks;
    explicit Input(std::vector<fs::path> paths){
        for(auto& path:paths){
            auto m=std::make_unique<Mapped>(path);if(!m->size){maps.push_back(std::move(m));continue;}
            auto p=m->data;auto end=p+m->size;auto lf=(const char*)memchr(p,'\n',end-p);p=lf?lf+1:end;
            while(p<end){auto q=p+std::min<size_t>(32*1024*1024,end-p);
                if(q<end){auto nl=(const char*)memchr(q,'\n',end-q);q=nl?nl+1:end;}
                tasks.push_back({p,q});p=q;}
            maps.push_back(std::move(m));
        }
    }
};
static std::vector<fs::path> csvfiles(const fs::path& dir){std::vector<fs::path> v;for(auto& e:fs::directory_iterator(dir))if(e.is_regular_file()&&e.path().extension()==".csv")v.push_back(e.path());std::sort(v.begin(),v.end());return v;}
template<class F>static void lines(Task task,F fn){auto p=task.begin;while(p<task.end){auto lf=(const char*)memchr(p,'\n',task.end-p);auto e=lf?lf:task.end;fn(SV(p,e-p));p=lf?lf+1:task.end;}}
// No repairing malformed rows; a CR remains an ordinary field byte.
static bool fields(SV row,SV* f,unsigned count){
    size_t at=0;
    for(unsigned i=0;i<count;i++){
        size_t e=row.find(',',at);if(e==SV::npos)e=row.size();
        SV x=row.substr(at,e-at);
        if(!x.empty()&&x.front()=='"'){
            if(x.size()<2||x.back()!='"')return false;
            x.remove_prefix(1);x.remove_suffix(1);if(x.find('"')!=SV::npos)return false;
        }else if(x.find('"')!=SV::npos)return false;
        f[i]=x;if(i+1<count){if(e==row.size())return false;at=e+1;}else if(e!=row.size())return false;
    }return true;
}
static int64_t days_from_civil(int y,unsigned m,unsigned d){
    y-=m<=2;const int era=(y>=0?y:y-399)/400;const unsigned yoe=y-era*400;
    const unsigned doy=(153*(m+(m>2?-3:9))+2)/5+d-1;
    const unsigned doe=yoe*365+yoe/4-yoe/100+doy;
    return era*146097LL+doe-719468;
}
static int64_t timestamp(SV s){
    if(s.empty())return BAD;
    if(s.size()!=20&&s.size()!=25){
        uint64_t value=0;for(unsigned char c:s){if(c<'0'||c>'9')return BAD;auto x=c-'0';if(value>(uint64_t(INT64_MAX)-x)/10)return BAD;value=value*10+x;}return (int64_t)value;
    }
    // Numeric timestamps of any length are valid syntax as well.
    bool digits=true;for(auto c:s)if(c<'0'||c>'9'){digits=false;break;}
    if(digits){uint64_t v=0;for(auto c:s){auto x=c-'0';if(v>(uint64_t(INT64_MAX)-x)/10)return BAD;v=v*10+x;}return v;}
    if(s[4]!='-'||s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':')return BAD;
    for(int i:{0,1,2,3,5,6,8,9,11,12,14,15,17,18})if(s[i]<'0'||s[i]>'9')return BAD;
    auto two=[&](int i){return (s[i]-'0')*10+s[i+1]-'0';};
    int y=two(0)*100+two(2),m=two(5),d=two(8),hh=two(11),mm=two(14),ss=two(17);
    if(!y||m<1||m>12||d<1||hh>23||mm>59||ss>59)return BAD;
    constexpr int md[]={31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap=y%4==0&&(y%100!=0||y%400==0);if(d>md[m-1]+(m==2&&leap))return BAD;
    int offset=0;
    if(s.size()==20){if(s[19]!='Z')return BAD;}
    else{
        if((s[19]!='+'&&s[19]!='-')||s[22]!=':')return BAD;
        for(int i:{20,21,23,24})if(s[i]<'0'||s[i]>'9')return BAD;
        int oh=two(20),om=two(23);if(oh>23||om>59)return BAD;offset=(oh*60+om)*(s[19]=='-'?-1:1);
    }
    return days_from_civil(y,m,d)*DAY+(hh*3600LL+mm*60+ss)*1000-offset*60000LL;
}
static uint32_t price(SV s){if(s.empty())return 0;uint32_t v=0;for(auto c:s){if(c<'0'||c>'9')return 0;v=v*10+(c-'0');if(v>10000000)return 0;}return v;}
static int64_t day_of(int64_t t){auto d=t/DAY;return d-(t<0&&t%DAY!=0);}
static std::string readall(const fs::path& p){std::ifstream f(p,std::ios::binary);if(!f)throw std::runtime_error("Cannot read "+p.string());return {std::istreambuf_iterator<char>(f),{}};}
static std::string jsonvalue(const std::string& s,const std::string& k){auto p=s.find('"'+k+'"');if(p==s.npos)throw std::runtime_error("Missing manifest key");p=s.find(':',p);p=s.find('"',p);auto e=s.find('"',p+1);if(e==s.npos)throw std::runtime_error("Bad manifest");return s.substr(p+1,e-p-1);}
template<class F>static void parallel(size_t n,F fn){
    std::atomic<size_t> next{0};std::exception_ptr error;std::mutex mutex;
    std::vector<std::thread> pool;
    for(int t=0;t<workers;t++)pool.emplace_back([&,t]{try{for(;;){auto i=next.fetch_add(1);if(i>=n)break;fn(i,t);}}catch(...){std::lock_guard<std::mutex> lock(mutex);if(!error)error=std::current_exception();next=n;}});
    for(auto& t:pool)t.join();if(error)std::rethrow_exception(error);
}
struct Identity {
    FlatMap<uint32_t> ids;
    std::vector<uint32_t> parent,size,minimum;
    std::vector<SV> names;
    uint32_t add(SV s){uint32_t i=names.size();auto [p,added]=ids.insert(s,i);if(added){names.push_back(s);parent.push_back(i);size.push_back(1);minimum.push_back(i);}return *p;}
    uint32_t root(uint32_t x){while(parent[x]!=x){parent[x]=parent[parent[x]];x=parent[x];}return x;}
    void join(SV a,SV b){auto x=root(add(a)),y=root(add(b));if(x==y)return;if(size[x]<size[y])std::swap(x,y);parent[y]=x;size[x]+=size[y];if(names[minimum[y]]<names[minimum[x]])minimum[x]=minimum[y];}
    void seal(){ids.each([&](SV, uint32_t& v){v=minimum[root(v)];});parent.clear();size.clear();minimum.clear();}
    SV canonical(SV s)const{auto p=ids.find(s);return p?names[*p]:s;}
};
struct RawEvent{SV id,person;int64_t ts,ingest;uint32_t revenue,category;bool valid;};
struct Event{SV person;int64_t ts;uint32_t revenue,category;};
struct Click{SV person,id;int64_t ts;uint32_t campaign;};
struct SmallEvent{int64_t ts;uint32_t user,revenue,category;};
struct SmallClick{SV id;int64_t ts;uint32_t user,campaign;};
struct CampaignStat{int64_t sessions=0,purchasing=0,revenue=0,assists=0;};
using RawBuckets=std::array<std::vector<RawEvent>,SHARDS>;
using EventBuckets=std::array<std::vector<Event>,SHARDS>;
using ClickBuckets=std::array<std::vector<Click>,SHARDS>;

struct UserWriter {
    arrow::StringBuilder user,category;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions,events,revenue;
    void append(SV u,int32_t d,int64_t s,int64_t e,int64_t r,SV cat,bool has_category){
        check(user.Append(u.data(),u.size()));check(day.Append(d));check(sessions.Append(s));check(events.Append(e));check(revenue.Append(r));
        if(has_category)check(category.Append(cat.data(),cat.size()));else check(category.AppendNull());
    }
    void finish(const fs::path& path){
        auto schema=arrow::schema({arrow::field("user_id",arrow::utf8()),arrow::field("day",arrow::date32()),arrow::field("sessions",arrow::int64()),arrow::field("events",arrow::int64()),arrow::field("revenue_cents",arrow::int64()),arrow::field("top_category",arrow::utf8())});
        auto table=arrow::Table::Make(schema,{take(user.Finish()),take(day.Finish()),take(sessions.Finish()),take(events.Finish()),take(revenue.Finish()),take(category.Finish())});
        auto output=take(arrow::io::FileOutputStream::Open(path.string()));
        parquet::WriterProperties::Builder props;props.compression(parquet::Compression::UNCOMPRESSED);props.disable_statistics();props.disable_dictionary();props.enable_dictionary("user_id");props.enable_dictionary("top_category");props.enable_dictionary("day");
        check(parquet::arrow::WriteTable(*table,arrow::default_memory_pool(),output,262144,props.build()));check(output->Close());
    }
};

int main() {try{
    const fs::path data=getenv("DATA_DIR")?getenv("DATA_DIR"):"/data";
    const fs::path out=getenv("OUTPUT_DIR")?getenv("OUTPUT_DIR"):"/output";
    workers=std::clamp(getenv("NCPU")?std::stoi(getenv("NCPU")):8,1,32);
    auto manifest=readall(data/"manifest.json");auto start=timestamp(jsonvalue(manifest,"window_start")),end=timestamp(jsonvalue(manifest,"window_end"));
    if(start==BAD||end==BAD||end<=start)throw std::runtime_error("Invalid time window");
    int64_t firstday=day_of(start),ndays=day_of(end-1)-firstday+1;
    if(ndays>36600)throw std::runtime_error("Time window exceeds 100 years");
    Input links(csvfiles(data/"links"));Identity identity;
    for(auto task:links.tasks)lines(task,[&](SV row){SV f[2];if(fields(row,f,2)&&!f[0].empty()&&!f[1].empty())identity.join(f[0],f[1]);});
    identity.seal();log("identity graph sealed");
    Input products({data/"products.csv"}),campaign_input({data/"campaigns.csv"});
    FlatMap<uint32_t> product_ids(65536),category_ids(32),campaign_ids(4096);
    std::vector<SV> category_names{"unknown"},campaign_names{"organic"},channels{"organic"};category_ids.insert("unknown",0);
    for(auto task:products.tasks)lines(task,[&](SV row){SV f[2];if(!fields(row,f,2))throw std::runtime_error("Invalid products.csv");auto [p,added]=category_ids.insert(f[1],category_names.size());if(added)category_names.push_back(f[1]);product_ids.insert(f[0],*p);});
    for(auto task:campaign_input.tasks)lines(task,[&](SV row){SV f[2];if(!fields(row,f,2))throw std::runtime_error("Invalid campaigns.csv");uint32_t i=campaign_names.size();auto [p,added]=campaign_ids.insert(f[0],i);if(added){campaign_names.push_back(f[0]);channels.push_back(f[1]);}});
    Input event_input(csvfiles(data/"events")),click_input(csvfiles(data/"clicks"));
    std::vector<RawBuckets> raw(workers);std::vector<ClickBuckets> parsed_clicks(workers);
    // Parse all files, including unevenly sized parts, in independent 32 MiB tasks.
    parallel(event_input.tasks.size(),[&](size_t task,int t){
        lines(event_input.tasks[task],[&](SV row){
            SV f[7];if(!fields(row,f,7)||f[0].empty())return;
            RawEvent r{};r.id=f[0];r.ingest=timestamp(f[3]);r.ts=timestamp(f[2]);r.person=identity.canonical(f[1]);
            bool purchase=f[4]=="purchase";r.revenue=purchase?price(f[6]):0;auto cat=product_ids.find(f[5]);r.category=cat?*cat:0;
            r.valid=!f[1].empty()&&!f[5].empty()&&(purchase||f[4]=="view"||f[4]=="cart")&&r.ts!=BAD&&r.ts>=start&&r.ts<end&&r.ingest!=BAD;
            if(r.valid)r.valid=r.ingest-r.ts<=DAY&&(!purchase||r.revenue>0);
            // Invalid copies still participate in earliest-ingest deduplication.
            raw[t][hashstr(r.id)&(SHARDS-1)].push_back(r);
        });
    });
    log("strict native event parsing complete");
    parallel(click_input.tasks.size(),[&](size_t task,int t){
        lines(click_input.tasks[task],[&](SV row){
            SV f[4];if(!fields(row,f,4)||f[0].empty()||f[1].empty())return;auto c=campaign_ids.find(f[2]);if(!c)return;auto ts=timestamp(f[3]);if(ts==BAD||ts<start-WEEK||ts>=end)return;
            auto person=identity.canonical(f[1]);parsed_clicks[t][hashstr(person)&(SHARDS-1)].push_back({person,f[0],ts,*c});
            // Identical click duplicates are idempotent under the sweep below.
        });
    });
    std::array<EventBuckets,SHARDS> routed;
    parallel(SHARDS,[&](size_t shard,int){
        size_t count=0;for(auto& worker:raw)count+=worker[shard].size();
        size_t cap=16;while(cap<count*1.45)cap*=2;
        std::vector<uint64_t> slots(cap,0);std::vector<const RawEvent*> winners;winners.reserve(count);
        for(auto& worker:raw)for(const auto& e:worker[shard]){
            uint64_t h=hashstr(e.id);uint32_t tag=uint32_t(h>>32);
            size_t at=(h>>5)&(cap-1);
            while(slots[at]&&(uint32_t(slots[at]>>32)!=tag||!same(winners[uint32_t(slots[at])-1]->id,e.id)))at=(at+1)&(cap-1);
            if(!slots[at]){winners.push_back(&e);slots[at]=(uint64_t(tag)<<32)|uint32_t(winners.size());}
            else {auto& kept=winners[uint32_t(slots[at])-1];if(e.ingest<kept->ingest)kept=&e;}
        }
        for(auto e:winners)if(e->valid)routed[shard][hashstr(e->person)&(SHARDS-1)].push_back({e->person,e->ts,e->revenue,e->category});
        for(auto& worker:raw){std::vector<RawEvent>().swap(worker[shard]);}
    });
    log("exact event deduplication complete");
    fs::create_directories(out/"user_daily");fs::create_directories(out/"campaign_daily");
    // Only clear this implementation's predictable filenames, not other files.
    for(unsigned i=0;i<SHARDS;i++){fs::remove(out/"user_daily"/("part-"+std::to_string(i)+".parquet"));}
    fs::remove(out/"campaign_daily"/"part-0.parquet");
    std::vector<std::vector<CampaignStat>> campaign_stats(SHARDS,std::vector<CampaignStat>(campaign_names.size()*ndays));
    parallel(SHARDS,[&](size_t shard,int){
        FlatMap<uint32_t> user_ids(1024);std::vector<SV> names;
        auto intern=[&](SV person){auto [p,added]=user_ids.insert(person,names.size());if(added)names.push_back(person);return *p;};
        size_t n=0,nc=0;for(auto& v:routed)n+=v[shard].size();for(auto& v:parsed_clicks)nc+=v[shard].size();
        std::vector<SmallEvent> ev;ev.reserve(n);
        for(auto& v:routed){for(auto& e:v[shard])ev.push_back({e.ts,intern(e.person),e.revenue,e.category});std::vector<Event>().swap(v[shard]);}
        // Click-only people cannot produce any output: do not intern them.
        std::vector<SmallClick> cl;cl.reserve(nc);
        for(auto& v:parsed_clicks){for(auto& c:v[shard])if(auto u=user_ids.find(c.person))cl.push_back({c.id,c.ts,*u,c.campaign});std::vector<Click>().swap(v[shard]);}
        // Linear counting scatter by person instead of globally sorting strings.
        std::vector<size_t> eo(names.size()+1),co(names.size()+1);
        for(auto& e:ev)eo[e.user+1]++;for(auto& c:cl)co[c.user+1]++;
        for(size_t i=1;i<eo.size();i++){eo[i]+=eo[i-1];co[i]+=co[i-1];}
        std::vector<SmallEvent> grouped(ev.size());std::vector<SmallClick> gc(cl.size());auto ep=eo,cp=co;
        for(auto& e:ev)grouped[ep[e.user]++]=e;for(auto& c:cl)gc[cp[c.user]++]=c;
        std::vector<SmallEvent>().swap(ev);std::vector<SmallClick>().swap(cl);
        UserWriter writer;
        std::vector<int64_t> category_sum(category_names.size());std::vector<uint32_t> touched_categories;
        std::vector<int64_t> last_seen(campaign_names.size(),BAD);std::vector<uint8_t> active_flag(campaign_names.size(),0);std::vector<uint32_t> active,touched_campaigns;
        auto& stats=campaign_stats[shard];
        for(uint32_t u=0;u<names.size();u++){
            auto eb=grouped.begin()+eo[u],ee=grouped.begin()+eo[u+1];
            std::sort(eb,ee,[](auto& a,auto& b){return a.ts<b.ts;});
            auto cb=gc.begin()+co[u],ce=gc.begin()+co[u+1];
            std::sort(cb,ce,[](auto& a,auto& b){return a.ts!=b.ts?a.ts<b.ts:a.id<b.id;});
            int64_t daily_day=BAD,daily_sessions=0,daily_events=0,daily_revenue=0;auto ci=cb;
            int64_t credit_time=BAD;SV credit_id{};uint32_t credit_campaign=0;
            auto flush=[&](){
                if(daily_day==BAD)return;uint32_t best=0;int64_t best_revenue=-1;
                for(auto c:touched_categories){auto r=category_sum[c];if(r>best_revenue||(r==best_revenue&&category_names[c]<category_names[best])){best=c;best_revenue=r;}category_sum[c]=0;}
                writer.append(names[u],daily_day,daily_sessions,daily_events,daily_revenue,category_names[best],!touched_categories.empty());
                touched_categories.clear();daily_sessions=daily_events=daily_revenue=0;
            };
            for(auto pos=eb;pos!=ee;){
                auto se=pos+1;while(se!=ee&&se->ts-(se-1)->ts<=1800000)++se;
                int64_t session_start=pos->ts,day=day_of(session_start);size_t di=day-firstday;
                if(day!=daily_day){flush();daily_day=day;}
                int64_t revenue=0;
                for(auto e=pos;e!=se;e++)if(e->revenue){revenue+=e->revenue;if(!category_sum[e->category])touched_categories.push_back(e->category);category_sum[e->category]+=e->revenue;}
                daily_sessions++;daily_events+=se-pos;daily_revenue+=revenue;
                while(ci!=ce&&ci->ts<=session_start){
                    uint32_t c=ci->campaign;
                    if(last_seen[c]==BAD)touched_campaigns.push_back(c);
                    last_seen[c]=ci->ts;
                    if(!active_flag[c]){active_flag[c]=1;active.push_back(c);}
                    if(ci->ts>credit_time||(ci->ts==credit_time&&ci->id<credit_id)){credit_time=ci->ts;credit_id=ci->id;credit_campaign=c;}
                    ++ci;
                }
                uint32_t credited=(credit_time!=BAD&&session_start-credit_time<=WEEK)?credit_campaign:0;
                auto& cs=stats[credited*ndays+di];cs.sessions++;cs.purchasing+=revenue>0;cs.revenue+=revenue;
                size_t keep=0;for(auto c:active){if(session_start-last_seen[c]<=WEEK){stats[c*ndays+di].assists++;active[keep++]=c;}else active_flag[c]=0;}active.resize(keep);
                pos=se;
            }
            flush();for(auto c:touched_campaigns){last_seen[c]=BAD;active_flag[c]=0;}touched_campaigns.clear();active.clear();
        }
        writer.finish(out/"user_daily"/("part-"+std::to_string(shard)+".parquet"));
    });
    log("parallel temporal sweeps and user Parquet complete");
    arrow::StringBuilder campaign,channel;arrow::Date32Builder day;arrow::Int64Builder sessions,purchasing,revenue,assists;
    for(size_t c=0;c<campaign_names.size();c++)for(int64_t d=0;d<ndays;d++){
        CampaignStat s;for(auto& st:campaign_stats){auto& x=st[c*ndays+d];s.sessions+=x.sessions;s.purchasing+=x.purchasing;s.revenue+=x.revenue;s.assists+=x.assists;}
        if(!s.sessions&&!s.assists)continue;
        check(campaign.Append(campaign_names[c].data(),campaign_names[c].size()));check(channel.Append(channels[c].data(),channels[c].size()));check(day.Append(firstday+d));
        check(sessions.Append(s.sessions));check(purchasing.Append(s.purchasing));check(revenue.Append(s.revenue));check(assists.Append(s.assists));
    }
    auto schema=arrow::schema({arrow::field("campaign_id",arrow::utf8()),arrow::field("channel",arrow::utf8()),arrow::field("day",arrow::date32()),arrow::field("sessions",arrow::int64()),arrow::field("purchasing_sessions",arrow::int64()),arrow::field("revenue_cents",arrow::int64()),arrow::field("assisted_sessions",arrow::int64())});
    auto table=arrow::Table::Make(schema,{take(campaign.Finish()),take(channel.Finish()),take(day.Finish()),take(sessions.Finish()),take(purchasing.Finish()),take(revenue.Finish()),take(assists.Finish())});
    auto output=take(arrow::io::FileOutputStream::Open((out/"campaign_daily"/"part-0.parquet").string()));
    parquet::WriterProperties::Builder props;props.compression(parquet::Compression::UNCOMPRESSED);props.disable_statistics();
    check(parquet::arrow::WriteTable(*table,arrow::default_memory_pool(),output,262144,props.build()));check(output->Close());
    log("all output committed");return 0;
}catch(const std::exception& e){std::cerr<<"FATAL: "<<e.what()<<'\n';return 1;}}
