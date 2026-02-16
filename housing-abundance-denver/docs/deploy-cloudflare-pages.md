# Deploy to Cloudflare Pages

## 1) Push project to GitHub
If this folder is not in a repo yet, create one and push:

```bash
cd /Users/mark/.openclaw/workspace/projects/work/housing-abundance-denver
git init
git add .
git commit -m "Initial housing-abundance-denver site + pipeline"
# create GitHub repo, then:
git remote add origin <YOUR_REPO_URL>
git branch -M main
git push -u origin main
```

## 2) Create Cloudflare Pages project
1. Go to Cloudflare Dashboard → **Workers & Pages** → **Create** → **Pages**.
2. Connect your GitHub repo.
3. Build settings:
   - **Framework preset:** None
   - **Build command:** *(leave blank)*
   - **Build output directory:** `site`
4. Deploy.

## 3) Custom domain (optional)
- Pages project → **Custom domains** → add your domain/subdomain.

## 4) Manual data refresh flow
Any time you want new data:

```bash
cd /Users/mark/.openclaw/workspace/projects/work/housing-abundance-denver
python3 scripts/build_v1_pipeline.py
git add data/ site/data.v1.js
git commit -m "Refresh housing pipeline data"
git push
```

Cloudflare Pages will auto-deploy on push.

---

## Optional: CLI deploy (direct)
If you prefer CLI deploy instead of Git integration:

```bash
npm i -g wrangler
wrangler login
cd /Users/mark/.openclaw/workspace/projects/work/housing-abundance-denver
wrangler pages deploy site --project-name housing-abundance-denver
```

---

## Notes
- `site/_headers` includes basic security/cache headers.
- `wrangler.toml` is included for CLI/project consistency.
- v1 data source is permit-derived, so it primarily reflects under-construction + delivered inventory.
