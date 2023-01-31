# Terraform
An Infrastructure-as-Code that can build, change, and manage infrastructure in a safe, consistent, and repeatable way by defining resource configurations that you can version, reuse, and share.

0. Login with credential as in [https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md)
1. Create `main.tf` that has descriptive code about the resources
2. Create `variables.tf` that save all those variables used in main.tf
3. Execution step:
    * `terraform init` Initializes & configures the backend, installs plugins/providers, & checks out an existing configuration from a version control
    * `terraform plan` Matches/previews local changes against a remote state, and proposes an Execution Plan.
    * `terraform apply` Asks for approval to the proposed plan, and applies changes to cloud
    * `terraform destroy` Removes your stack from the Cloud


