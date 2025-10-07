#!/usr/bin/env python3
import argparse, collections, csv, math, os, sys, time
from typing import List, Optional, Tuple, Callable
import requests

API = "https://api.github.com/search/repositories"
API_VERSION = "2022-11-28"
MAX_PER_QUERY = 950  # keep below 1000-result cap

# ---------- helpers ----------
def mk_session(token: Optional[str]) -> requests.Session:
    s = requests.Session()
    h = {"Accept":"application/vnd.github+json",
         "X-GitHub-Api-Version":API_VERSION,
         "User-Agent":"lang-dist-research"}
    if token: h["Authorization"] = f"Bearer {token}"
    s.headers.update(h)
    return s

def coerce_bounds(lo:int, hi:Optional[int])->Tuple[int,Optional[int]]:
    return (hi,hi) if hi is not None and lo>hi else (lo,hi)

def build_q(metric:str, lo:int, hi:Optional[int], base_q:str)->str:
    lo,hi = coerce_bounds(lo,hi)
    return f"{metric}:>={lo} {base_q}" if hi is None else f"{metric}:{lo}..{hi} {base_q}"

def get_total(session:requests.Session, q:str)->int:
    p={"q":q,"per_page":1}
    while True:
        r=session.get(API,params=p)
        if r.status_code==403:
            ra=r.headers.get("Retry-After"); rs=r.headers.get("X-RateLimit-Reset")
            if ra: time.sleep(int(ra)+1); continue
            if rs:
                wait=max(0,int(rs)-int(time.time()))+1
                if wait>0: time.sleep(wait); continue
        if r.status_code!=200: raise RuntimeError(f"GitHub API error {r.status_code}: {r.text}")
        return int(r.json().get("total_count",0))

def fetch_range(session:requests.Session, q:str, need:int, seen:set)->List[dict]:
    total=min(get_total(session,q), need)
    items=[]; page=1
    while len(items)<total:
        p={"q":q,"sort":"stars","order":"desc","per_page":100,"page":page}
        while True:
            r=session.get(API,params=p)
            if r.status_code==403:
                ra=r.headers.get("Retry-After"); rs=r.headers.get("X-RateLimit-Reset")
                if ra: time.sleep(int(ra)+1); continue
                if rs:
                    wait=max(0,int(rs)-int(time.time()))+1
                    if wait>0: time.sleep(wait); continue
            if r.status_code!=200: raise RuntimeError(f"GitHub API error {r.status_code}: {r.text}")
            data=r.json(); break
        for repo in data.get("items",[]):
            if repo.get("id") in seen: continue
            seen.add(repo.get("id")); items.append(repo)
            if len(items)>=total: break
        page+=1
    return items

def find_next_lower(count_fn:Callable[[int,Optional[int]],int], upper:Optional[int], cap:int=MAX_PER_QUERY)->int:
    if upper is None:
        lo,hi=0,1
        while count_fn(hi,None)>cap and hi<1_000_000: hi*=2
        best=hi
        while lo<=hi:
            mid=(lo+hi)//2
            if count_fn(mid,None)<=cap: best=mid; hi=mid-1
            else: lo=mid+1
        return max(0,best)
    lo,hi=0,max(0,upper)
    if count_fn(hi,upper)>cap: return hi
    best=hi
    while lo<=hi:
        mid=(lo+hi)//2
        if count_fn(mid,upper)<=cap: best=mid; hi=mid-1
        else: lo=mid+1
    return max(0,best)

# ---------- core ----------
def collect_top(session:requests.Session, n:int, base_q:str, metric:str)->List[dict]:
    def count_fn(lo:int, hi:Optional[int])->int:
        return get_total(session, build_q(metric, lo, hi, base_q))
    seen=set(); upper=None; out=[]
    while len(out)<n:
        lower=find_next_lower(count_fn, upper, MAX_PER_QUERY)
        if upper is not None and lower>upper: lower=upper
        need=n-len(out)
        q=build_q(metric, lower, upper, base_q)
        out.extend(fetch_range(session, q, need, seen))
        if lower<=0: break
        upper=lower-1
    key = (lambda r:(r.get("size") or 0, r.get("full_name") or "")) if metric=="size" \
          else (lambda r:(r.get("stargazers_count") or 0, r.get("full_name") or ""))
    out.sort(key=key, reverse=True)
    return out[:n]

def is_unknown(repo:dict)->bool:
    lang=repo.get("language")
    return lang is None or (isinstance(lang,str) and not lang.strip())

def lang_distribution(repos:List[dict], exclude_unknown:bool=False):
    ctr=collections.Counter(); included=[]; unknowns=[]
    for r in repos:
        if is_unknown(r):
            unknowns.append(r)
            if exclude_unknown: continue
            lang="Unknown"
        else: lang=r.get("language") or "Unknown"
        ctr[lang]+=1; included.append(r)
    return ctr,sum(ctr.values()),unknowns,included

def md_table(counter:collections.Counter, total:int, top:int)->str:
    rows=counter.most_common() if top==0 else counter.most_common(top)
    md=["| Language | Count | Percent |","|:--|--:|--:|"]
    for lang,count in rows:
        pct=100.0*count/max(1,total)
        md.append(f"| {lang} | {count} | {pct:.2f}% |")
    if top and len(counter)>top:
        shown=sum(c for _,c in rows); other=total-shown
        md.append(f"| (others) | {other} | {100.0*other/max(1,total):.2f}% |")
    return "\n".join(md)

def write_unknown_csv(unknowns:List[dict], path:str):
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["full_name","language","stargazers_count","size_kib","html_url"])
        for r in unknowns:
            w.writerow([r.get("full_name",""),r.get("language") or "",
                        r.get("stargazers_count") or 0,r.get("size") or 0,
                        r.get("html_url") or ""])

# ---------- cli ----------
def main():
    ap=argparse.ArgumentParser(description="Markdown-friendly language distribution among top GitHub repos.")
    ap.add_argument("--n", type=int, default=5000)
    ap.add_argument("--rank-by", choices=["stars","size"], default="stars")
    ap.add_argument("--include-forks", action="store_true")
    ap.add_argument("--exclude-unknown", action="store_true")
    ap.add_argument("--unknown-out", type=str, default="")
    ap.add_argument("--top", type=int, default=30)
    args=ap.parse_args()

    token=os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("[warning] No GITHUB_TOKEN set; unauthenticated requests will be slow.", file=sys.stderr)
    session=mk_session(token)
    base_q="fork:true" if args.include_forks else "fork:false"

    repos=collect_top(session,args.n,base_q,args.rank_by)
    counter,total,unknowns,included=lang_distribution(repos,exclude_unknown=args.exclude_unknown)

    # Markdown output
    print(f"# Language Distribution among Top {args.n} Repos (by {args.rank_by})\n")
    print(md_table(counter,total,args.top))

    if args.unknown_out and unknowns:
        write_unknown_csv(unknowns,args.unknown_out)
        print(f"\n_Wrote {len(unknowns)} unknown-language repos to **{args.unknown_out}**_")

    # Markdown summary
    if included:
        stars=[r.get("stargazers_count") or 0 for r in included]
        sizes=[r.get("size") or 0 for r in included]
        smin,smax=min(stars),max(stars); szmin,szmax=min(sizes),max(sizes)
        print("\n## Summary\n")
        print(f"- **Analyzed repos:** {len(included)} (ranked by `{args.rank_by}`)")
        if unknowns:
            msg="(excluded)" if args.exclude_unknown else '(included as `"Unknown"`)'
            print(f"- **Unknown-language repos:** {len(unknowns)} {msg}")
        print(f"- **Stars range:** {smin:,} – {smax:,}")
        print(f"- **Max Size:** {szmax/1024/1024:.1f} GB")
    else:
        print("\n_No repositories included after filters._")

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr); sys.exit(130)
