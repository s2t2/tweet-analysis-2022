



# Deployer's Guide - Tweet Streaming

## Server Setup

Navigate to the root directory of this repo first.

Create a new app server (first time only):

```sh
heroku create tweet-streaming-2022 # (or use your own app name here)

heroku create tweet-streaming-2022 -r streaming
```

Verify remote association:

```sh
git remote -v
```


Set buildpacks:

```sh
heroku buildpacks:set heroku/python
heroku buildpacks:add https://github.com/s2t2/heroku-google-application-credentials-buildpack
# create env var on the server that looks like your credentials (from your local file):
heroku config:set GOOGLE_CREDENTIALS="$(< google-credentials.json)" # references local creds file
# tell the google app to use the file created by the buildpack
heroku config:set GOOGLE_APPLICATION_CREDENTIALS="google-credentials.json" # references server creds
```


Set env vars on the server:

```sh
# server config:
heroku config:set APP_ENV="production"
#heroku config:set SERVER_NAME="your-server-name-here"

# credentials:
heroku config:set TWITTER_BEARER_TOKEN="....."

# storage config:
heroku config:set STORAGE_MODE="bq"
heroku config:set DATASET_ADDRESS="YOUR_PROJECT.YOUR_DATASET"

# job config:
heroku config:set BATCH_SIZE="50"
```


## Process Management

Add an entry for the `tweet_streaming_job` to the Procfile (see Procfile).

Setup dyno for the `tweet_streaming_job`.


## Deploying

Deploy / upload code to the server:

```sh
git push streaming main

# git push streaming mybranch:main
```

Viewing the logs:

```sh
heroku logs --tail
```
