# Darwin Compute Script

## Compute Script

Status Poller, Auto Termination and Jupyter Pods Management Scripts for Darwin Compute.

Status Poller also includes cluster timeout job.

* Scheduler Documentation
* Status Poller Documentation
* Auto Termination Documentation
* Cluster Timeout Job Documentation

## Darwin Compute CRON Jobs

### Environment Variables

| Environment Variable         | Description                   |
|------------------------------|-------------------------------|
| ENV                          | Environment (stag, uat, prod) |
| TEAM_SUFFIX                  | Odin Team Suffix              |
| VPC_SUFFIX                   | Odin VPC Suffix               |
| VAULT_SERVICE_SLACK_TOKEN    | Slack Token for Alerts        |
| VAULT_SERVICE_SLACK_USERNAME | Slack Username for Alerts     |

### K8S Cluster Reconciliation

K8S reconciliation job which directly pulls clusters from k8s and check if it is available in Darwin DB or not.
If not available, it will throw a slack alert in _**darwin-cost-alerts**_ channel.

### Long Running Clusters Monitoring

Long Running Clusters Monitoring job checks which clusters are running for more than 24 hours
and throws a slack alert in _**darwin-cost-alerts**_ channel.
