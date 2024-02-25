# Contributing

We welcome and greatly appreciate contributions! Every small contribution is valuable, and we make sure to give credit 
where it's due.

## Contribution Agreement

As a contributor, you represent that the code you submit is your original work or that of your employer (in which case 
you represent you have the right to bind your employer).  By submitting code, you (and, if applicable, your employer) 
are licensing the submitted code to LinkedIn and the open source community subject to the BSD 2-Clause license.

In short, our goal is to wow the data infrastructure customers across the industry, and we are open about sharing work 
with each other.

## Responsible Disclosure of Security Vulnerabilities

Please refer to our [Security Policy](SECURITY.md) for our policy on responsibly reporting security vulnerabilities.

## Pull Requests

### Setting up your local development environment

First, you would need to [Fork](https://help.github.com/articles/about-forks/) the repository [openhouse](https://github.com/linkedin/openhouse) 
to your own GitHub account. Then, you can clone the fork to your local machine.

```
git clone git@github.com:your-username/openhouse.git 
cd openhouse
```

To prepare yourself for a contribution, you would need to install and get familiar in a local environment as 
described in [Setup](SETUP.md). To deploy the services to your Kubernetes cluster, you can follow the instructions in
[Deploy](DEPLOY.md).

You will not be able to push directly to the `main` branch. You will need to create a new branch and push to your fork.
You can then create a pull request from your branch in your fork in the [openhouse](https://github.com/linkedin/openhouse)
repository.

### Issues and Pull Request

We use Github Issues to track and manage outstanding features, bugs, or other improvements. We request each pull request
to call out the issue it is addressing. If there is no issue, please create one first and then create a pull request.

### Pull Request Template

Please refer to our [Pull Request Template](.github/pull_request_template.md) for our policy on what we expect.

### Types of Contributions

[1] Bug Fixes (#bug): Create a new issue with a tag `bug` and add details like show [here](.github/ISSUE_TEMPLATE/bug_report_template.yaml). 

[2] New Features (#feat): Create a new issue with a tag `feat` and add details like show [here](.github/ISSUE_TEMPLATE/feature_request_template.yaml).
Once there is an issue please engage with the maintainers to discuss the feature and drive alignment on the need and on 
the execution plan before starting the coding work. If this is a large feature, identify a champion (one of the existing
maintainers) who will help through the feature delivery, define an execution plan that includes breakdown of feature 
PR into smaller PRs. Please expect more scrutiny if there are API changes. For any API changes, the [specs](docs/specs) need
to be updated, see instructions [here](docs/specs/README.md).

[3] Documentation (#docs): Kinds of documentation include README, API documentation, Documentation website etc.

#### Documentation Website Contribution

Documentation website is hosted on a different branch [docsite](https://github.com/linkedin/openhouse/tree/docsite). Instructions to build the documentation site
can be found at the root [README.md](https://github.com/linkedin/openhouse/blob/docsite/README.md). 

Create a PR against the branch `docsite` for your documentation website changes. When changes are merged into `docsite`, a GitHub Action will automatically publish static HTML files to the `gh_pages` branch. These files will then be deployed to the website. Note: `gh_pages` is a gh-action built branch, PR shouldn't be raised against it.


## Code of Conduct

Please refer to our [Code of Conduct](CODE_OF_CONDUCT.md) for our policy on how we expect contributors to behave.

Please join [OpenHouse Slack Workspace](https://join.slack.com/t/openhouse-bap9266/shared_invite/zt-2bsi0t8pi-wUOeDvQr8j8d5yl3X8WQJQ)
if you have questions on the contribution process or have proposal to improve the process.