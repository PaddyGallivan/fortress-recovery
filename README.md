# fortress-recovery

Cloudflare Worker `fortress-recovery` — push to `main` deploys via GitHub Actions. **NO LOCAL INFRA.**

## One-time setup
1. Tick `workflow` scope on the GH PAT (or use a fine-grained one)
2. `cp docs/SETUP-WORKFLOW.yml .github/workflows/deploy.yml` and push

Secrets `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` are pre-set on this repo.
Worker secrets stay in CF; deploys use `--keep-vars`.
