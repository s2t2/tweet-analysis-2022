



import os
#from uuid import uuid4

#from dotenv import load_dotenv
import requests

from app import seek_confirmation
from app.tweet_collection.job import STORAGE_MODE #, Job
from app.tweet_collection.db import CollectionDatabase
#from app.tweet_collection.bq import BigQueryDatabase

#load_dotenv()

#MEDIA_STORAGE_MODE = os.getenv("MEDIA_STORAGE_MODE", default="local")
MEDIA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "media")

#class MediaStorage:
#    def __init__(self):
#        pass


class Job:
    #def __init__(self, storage_mode=STORAGE_MODE, media_storage_mode=MEDIA_STORAGE_MODE, media_limit=None):
    def __init__(self, storage_mode=STORAGE_MODE, media_limit=None):
        self.storage_mode = storage_mode
        if self.storage_mode == "sqlite":
            self.db = CollectionDatabase(destructive=False)
        #elif self.storage_mode == "bq":
        #    self.db = BigQueryDatabase()
        else:
            raise AttributeError("oops wrong storage mode")

        #self.media_storage_mode = media_storage_mode
        #if self.media_storage_mode == "local":
        #    self.media_store = MediaStorage()
        ##elif self.storage_mode == "gcs":
        ##    self.media_store = GoogleCloudStorageService()
        #else:
        #    raise AttributeError("oops wrong media storage mode")

        print("------------------")
        print("MEDIA COLLECTION JOB...")

        seek_confirmation()


    def download_media(self, url, local_filepath):
        response = requests.get(url)
        with open(local_filepath, "wb") as media_file:
            media_file.write(response.content)

    def perform(self):
        for row in self.db.get_downloadable_media():
            media_key = row["media_key"]
            #media_type = row["media_type"]
            media_url = row["media_url"]
            preview_url = row["preview_image_url"]

            print("DOWNLOADING...")

            # DOWNLOAD MEDIA
            if media_url:
                #filename = f"{media_type}_{media_key}_media.jpg"
                filename = f"{media_key}_media.jpg"
                print("... ", filename)
                local_filepath = os.path.join(MEDIA_DIR, filename)
                self.download_media(url=media_url, local_filepath=local_filepath)

            # DOWNLOAD PREVIEW
            if preview_url:
                #filename = f"{media_type}_{media_key}_preview.jpg"
                filename = f"{media_key}_preview.jpg"
                print("... ", filename)
                local_filepath = os.path.join(MEDIA_DIR, filename)
                self.download_media(url=preview_url, local_filepath=local_filepath)





if __name__ == "__main__":


    job = Job()

    job.perform()
