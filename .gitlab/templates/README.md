# GitLab CI Templates

Plantillas reutilizables para GitLab CI/CD.

## Pre-commit Template

Ejecuta pre-commit hooks con auto-formateo en merge requests.

```yaml
include:
  - local: '.gitlab/templates/pre-commit.yml'

pre-commit:
  extends: .pre-commit-template
  variables:
    GIT_USER_EMAIL: "ci@example.com"
    GIT_USER_NAME: "CI Bot"
    GIT_BOT_USER: "${CI_NOPROTECTED_BOT_USER}"
    GIT_TOKEN_VAR: "CI_NOPROTECTED_TOKEN"
    GIT_REPO_URL: "https://${CI_NOPROTECTED_BOT_USER}:${CI_NOPROTECTED_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"
```

## Auto-bump Template

Versionado semántico automático con commitizen.

```yaml
include:
  - local: '.gitlab/templates/auto-bump.yml'

auto-bump:
  extends: .auto-bump-template
  variables:
    GIT_USER_EMAIL: "ci@example.com"
    GIT_USER_NAME: "CI Bot"
    DEFAULT_BRANCH: "main"
    GIT_BOT_USER: "${CI_PROTECTED_BOT_USER}"
    GIT_TOKEN_VAR: "CI_PROTECTED_TOKEN"
    GIT_REPO_URL: "https://${CI_PROTECTED_BOT_USER}:${CI_PROTECTED_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"
```

## Variables Requeridas

Configura estas variables en Settings > CI/CD > Variables:

- `CI_NOPROTECTED_TOKEN` - Token para merge requests
- `CI_NOPROTECTED_BOT_USER` - Bot user para token no protegido
- `CI_PROTECTED_TOKEN` - Token para rama principal
- `CI_PROTECTED_BOT_USER` - Bot user para token protegido

Los bot users se generan automáticamente al crear Project Access Tokens en GitLab.

## Ejemplo

```yaml
stages:
  - lint
  - auto-bump

include:
  - local: '.gitlab/templates/pre-commit.yml'
  - local: '.gitlab/templates/auto-bump.yml'

pre-commit:
  extends: .pre-commit-template
  variables:
    GIT_USER_EMAIL: "ci@company.com"
    GIT_USER_NAME: "CI Bot"
    GIT_BOT_USER: "${CI_NOPROTECTED_BOT_USER}"
    GIT_TOKEN_VAR: "CI_NOPROTECTED_TOKEN"
    GIT_REPO_URL: "https://${CI_NOPROTECTED_BOT_USER}:${CI_NOPROTECTED_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"

auto-bump:
  extends: .auto-bump-template
  variables:
    GIT_USER_EMAIL: "ci@company.com"
    GIT_USER_NAME: "CI Bot"
    DEFAULT_BRANCH: "main"
    GIT_BOT_USER: "${CI_PROTECTED_BOT_USER}"
    GIT_TOKEN_VAR: "CI_PROTECTED_TOKEN"
    GIT_REPO_URL: "https://${CI_PROTECTED_BOT_USER}:${CI_PROTECTED_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"
```
