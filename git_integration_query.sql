CREATE OR REPLACE API INTEGRATION my_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Abhibangal')
   ALLOWED_AUTHENTICATION_SECRETS = (my_git_secret)
  ENABLED = TRUE;


  CREATE OR REPLACE SECRET MY_GIT_SECRET
  TYPE= PASSWORD
  USERNAME = ABHIBANGAL
  PASSWORD = '<GIT PAT >';

  SHOW  INTEGRATIONS;
  SHOW SECRETS;
/*Make sure the git repostiory ur creating should be Public or should have releveant access to clone to repo*/
  CREATE OR REPLACE GIT REPOSITORY  S3_TO_SF_GIT_REPO
  ORIGIN = 'https://github.com/Abhibangal/s3-to-snowflake-ingestion.git'
  API_INTEGRATION = my_git_api_integration
  GIT_CREDENTIALS = MY_GIT_SECRET;

  ALTER GIT REPOSITORY S3_TO_SF_GIT_REPO FETCH;
  

  LS @CT_PROTOCOL.LINKML.S3_TO_SF_GIT_REPO/main;