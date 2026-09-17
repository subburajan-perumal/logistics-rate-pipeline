# EKS runbook — one 3-hour window (Phase 7)

Not yet executed. Fill the timestamp table in as you go; the next-day bill
for tag `project=logistics-rate-pipeline` goes into `docs/PLAN.md`
Appendix A and §20.

Pre-flight (do **before** the window opens):

- [ ] `aws sts get-caller-identity` works; AWS Budget alarm at $5 exists
- [ ] bucket exists; `terraform -chdir=deploy/terraform/eks validate` passes
- [ ] `deploy/helm/rate-pipeline/values-eks.yaml`: `ingestd.s3.bucket` set
- [ ] GHCR images published by CI (`ghcr.io/subburajan-perumal/logistics-rate-pipeline/{ingestd,mocksources}:latest`)
- [ ] Kubernetes minor in `main.tf` is in **standard** support today

| step | command | started | finished | notes |
|---|---|---|---|---|
| 1 | `terraform -chdir=deploy/terraform/eks apply -var bucket=<b>` (≈ 12 min) | | | |
| 2 | `aws eks update-kubeconfig --name rates --region us-east-1` | | | |
| 3 | `helm upgrade --install ingress-nginx ingress-nginx --repo https://kubernetes.github.io/ingress-nginx -n ingress-nginx --create-namespace --set controller.service.type=LoadBalancer` | | | NLB hostname → `kubectl -n ingress-nginx get svc` |
| 4 | `kubectl create ns rates && kubectl -n rates create secret generic ingestd-secrets --from-literal=EVENTIDE_API_KEY=eventide-demo-key` | | | no AWS keys — Pod Identity |
| 5 | `helm upgrade --install rates deploy/helm/rate-pipeline -n rates -f deploy/helm/rate-pipeline/values-eks.yaml --set ingestd.ingress.host=ingest.<nlb-ip>.nip.io --wait` | | | |
| 6 | `curl -X POST http://ingest.<…>.nip.io/runs` → poll `GET /runs` | | | run id: |
| 7 | `aws s3 ls s3://<b>/runs/run_id=<id>/ --recursive` | | | manifest present? parts? |
| 8 | `kubectl -n rates top pod` during a second run | | | peak Mi: |
| 9 | `bash tests/e2e/shutdown.sh rates rates` (fs sink is off on EKS → check `aws s3 ls` for `_tmp/` and no manifest instead) | | | |
| 10 | `helm uninstall rates -n rates && helm uninstall ingress-nginx -n ingress-nginx` (wait for the NLB to go) | | | |
| 11 | `terraform -chdir=deploy/terraform/eks destroy -var bucket=<b>` | | | **hard stop at T+2h30** |
| 12 | next day: Cost Explorer filtered by tag | | | $ |

Manual teardown checklist if `destroy` fails: NLB (EC2 → Load Balancers),
node group, cluster, addons, IAM roles `rates-eks-*`/`rates-ingestd`,
VPC (IGW, subnets, route table).
