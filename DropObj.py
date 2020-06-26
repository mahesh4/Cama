import dropbox
import glob
import os.path
import sys

from dropbox.files import WriteMode
from db_connect import DBConnect
ACCESS_TOKEN = "P0l57Ue0sVAAAAAAAAAAXFpFpLuSBdt6rOGnBClN4D5kbuaT_Vg83AiddUFJSEuh"


def upload_output(output_flow_type):
    db = DBConnect()
    dbx = dropbox.Dropbox(ACCESS_TOKEN)
    try:
        db.connect_db()
        MONGO_CLIENT = db.get_connection()
        database = MONGO_CLIENT["output"]
        files_collection = database["files"]

        if output_flow_type == "preflow":
            output = files_collection.find_one({"status": "running", "flow": "preflow"})

            if output is None:
                raise Exception("record doesn't exist")

            folder_name = "/" + output["folder_name"]
            output_path = "/var/lib/model/CaMa_Pre/out/hamid"
        else:
            output = files_collection.find_one({"status": "running", "flow": output_flow_type})

            if output is None:
                raise Exception("record doesn't exist")

            folder_name = "/" + output["folder_name"]
            output_path = "/var/lib/model/CaMa_Post/out/hamid"

        # Inserting the folder into dropbox
        dbx.files_create_folder_v2(folder_name, autorename=False)

        # output_path = "/Users/magesh/Downloads/flood/post_flow_wetland"  # FOR DEBUG
        # Uploading the results
        for filename in glob.glob(os.path.join(output_path, '*.bin')):
            with open(filename, 'rb') as fp:
                dbx.files_upload(fp.read(), folder_name + "/" + filename.split("/")[-1], mode=WriteMode("overwrite"))
                fp.close()

        # updating database to set status to completed
        files_collection.update({"_id": output["_id"]}, {"$set": {"status": "completed"}})

    except Exception as e:
        raise e
    finally:
        db.disconnect_db()


def download_file(folder_name, file_name):
    try:
        dbx = dropbox.Dropbox(ACCESS_TOKEN)
        path = "/" + folder_name + "/" + file_name
        dbx.files_download_to_file(os.path.join(os.getcwd(), "files", file_name), path)

    except Exception as e:
        raise e


if __name__ == "__main__":
    input = sys.argv
    if len(input) == 2:
        if input[1] in ["preflow", "postflow_wetland", "postflow_groundwater"]:
            upload_output(input[1])
    else:
        print("invalid arguments passed")
