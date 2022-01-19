#!/usr/bin/env python3
##
## Author: Rich Gannon <rich@richgannon.net>
##
## Purpose: Keep track of video collection for search and backup purposes
##
## To do:
## > Checksum files
##   - Takes FOREVER
##   - After finalizing database structure and no longer dropping table every import
## > Include TV show episodes
## > Pretty pandas output
##   - How to transform mtime and size before output?
## > Add function for requested videos
## > Add function for video problems (i.e. bad subtitles, bad quality)
## > Web interface
##


##
## Begin Configuration
##
mysql_user = "pyVideos"
mysql_password = "Q*8W/J6fOvujz@Ck"
mysql_host = "10.23.60.20"
mysql_database = "pyVideos"
resolutions = ['360p', '480p', '720p', '1080p', '4k']
root = "/mnt/nfs/videos/Movies"
##
## End configuration
##

from mysql.connector import connect, Error
import os
import subprocess
import datetime

def mysql_open():
    global db_conn, db
    try:
        db_conn = connect(host = mysql_host, user = mysql_user, passwd = mysql_password, db = mysql_database)
    except Error as err:
        quit(err)

    db = db_conn.cursor()


def mysql_close():
    db.close()
    db_conn.close()


#def clear_screen():
#   if os.name == 'posix':
#      _ = os.system('clear')


def get_response():
    print("\n"
            " s: Search movies from database\n"
            " c: Compare current movies to database\n"
            " q: Quit\n"
            "\n")

    return input(" Selection: ").lower()


def do_import():
    mysql_open()
    print("Initializing table ...")
    db.execute("CREATE OR REPLACE TABLE videos"
            "(id SMALLINT(5) NOT NULL AUTO_INCREMENT,"
            "name VARCHAR(80) NOT NULL,"
            "size BIGINT(11) NOT NULL,"
            "mtime int(10) NOT NULL,"
            "res_folder enum('360p','480p','720p','1080p','4k'),"
            "x SMALLINT(4) NOT NULL,"
            "y SMALLINT(4) NOT NULL,"
            "video_codec ENUM('h264','hevc','mpeg2video','mpeg4','msmpeg4v3','vc1') NOT NULL,"
            "audio_codec ENUM('aac','ac3','dts','eac3','flac','mp3','opus','truehd','unknown') NOT NULL,"
            "audio_channels ENUM('1','2','5','6','7','8') NOT NULL,"
            "audio_layout ENUM('5.0','5.1','6.1','7.1','mono','stereo','unknown') NOT NULL,"
            "PRIMARY KEY (id))")
    print("Done")
    for curr_resolution in resolutions:
        print(f"Importing videos from {curr_resolution} ...")
        dir_videos = os.listdir(f"{root}/{curr_resolution}")
        video_dict = {}
        for file_name in dir_videos:
            file_size = os.path.getsize(f"{root}/{curr_resolution}/{file_name}")
            file_mtime = int(os.path.getmtime(f"{root}/{curr_resolution}/{file_name}"))
            real_res = str(subprocess.check_output(f"ffprobe -v quiet -select_streams v -show_entries stream=codec_name,width,height -of csv=p=0:s=, \"{root}/{curr_resolution}/{file_name}\" | head -n1", shell=True))
            real_res = real_res.split("'")
            real_res = real_res[1].replace("\\n", "")
            v_codec, x_val, y_val = real_res.split(",")
            audio = str(subprocess.check_output(f"ffprobe -v quiet -select_streams a -show_entries stream=codec_name,channels,channel_layout -of csv=p=0:s=, \"{root}/{curr_resolution}/{file_name}\" | head -n1", shell=True))
            audio = audio.split("'")
            audio = audio[1].replace("\\n", "")
            if audio == "":
                a_codec = "unknown"
                a_channels = 2
                a_layout = "unknown"
            else:
                a_codec, a_channels, a_layout = audio.split(",")
                a_layout = a_layout.replace("(side)", "")
            db.execute(f"INSERT INTO videos (name, size, mtime, res_folder, x, y, video_codec, audio_codec, audio_channels, audio_layout)"
                    "VALUES ("
                    f"'{file_name}',"
                    f"'{file_size}',"
                    f"'{file_mtime}',"
                    f"'{curr_resolution}',"
                    f"'{x_val}',"
                    f"'{y_val}',"
                    f"'{v_codec}',"
                    f"'{a_codec}',"
                    f"'{a_channels}',"
                    f"'{a_layout}')")
        db_conn.commit()
        print("Done")

    db.execute("ALTER TABLE videos ADD INDEX (res_folder)")
    db.execute("ALTER TABLE videos ADD INDEX (video_codec)")
    db.execute("ALTER TABLE videos ADD INDEX (audio_codec)")
    db.execute("ALTER TABLE videos ADD INDEX (audio_channels)")
    db.execute("ALTER TABLE videos ADD INDEX (audio_layout)")
    db_conn.commit()
    mysql_close()


def do_search(search):
    mysql_open()
    print()
    db.execute(f"SELECT name, size, mtime, res_folder, x, y FROM videos WHERE name LIKE '%{search}%' ORDER BY name")
    result = db.fetchall()
    mysql_close()
    #df = pd.DataFrame.from_records(db, columns=[i[0] for i in db.description])
    #print(df)
    for video in result:
        res = str(video[3])
        res = res.strip("{'")
        res = res.strip("'}")
        name = video[0]
        size = int(video[1] / 1024 / 1024)
        mtime = datetime.datetime.fromtimestamp(video[2])
        x = video[4]
        y = video[5]
        print(f"{name} | {res} | {size} MB | {mtime} | {x}x{y}")


def do_compare():
    problem_videos = []
    total_size = 0
    print()
    mysql_open()
    for curr_resolution in resolutions:
        dir_videos = os.listdir(f"{root}/{curr_resolution}")
        db.execute(f"SELECT name, mtime, size FROM videos WHERE res_folder = '{curr_resolution}'")
        result = db.fetchall()
        for video in result:
            if video[0] in dir_videos:
                total_size += video[2]
                file_size = os.path.getsize(f"{root}/{curr_resolution}/{video[0]}")
                file_mtime = int(os.path.getmtime(f"{root}/{curr_resolution}/{video[0]}"))
                if video[1] != file_mtime:
                    print(f" !!! MTIME MISMATCH >>> {video[0]}")
                    problem_videos.append([video[0], curr_resolution, "mtime mismatch"])
                else:
                    print(f" OK >>> {curr_resolution}/{video[0]}")
            else:
                print(f" !!! MISSING >>> {video[0]}")
                problem_videos.append([video[0], curr_resolution, "file missing"])
    mysql_close()
    total_size = round(total_size / 1024 / 1024 / 1024, 1)
    print("\n"
        f" Total size: {total_size} GB\n"
        "\n")

    if not problem_videos:
        print(" No videos with issues.")
    else:
        print(" Videos with problems:")
        for video in problem_videos:
            print(f" --> {video[1]}/{video[0]} with error: {video[2]}")
    print()


response = 0
while response != "q":
    response = get_response()
    if response == "n":
        continue
    elif response == "import":
       do_import()
    elif response == "s":
        print()
        do_search(input("Search string: "))
    elif response == "c":
        do_compare()

