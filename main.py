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
root = "/mnt/nfs/videos"
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


def do_import_movies():
    mysql_open()
    print("Initializing table ...")
    db.execute("CREATE OR REPLACE TABLE movies"
            "(id SMALLINT(5) NOT NULL AUTO_INCREMENT,"
            "name VARCHAR(80) NOT NULL,"
            "size BIGINT(11) NOT NULL,"
            "mtime INT(10) NOT NULL,"
            "res_folder ENUM('360p','480p','720p','1080p','4k'),"
            "x SMALLINT(4) NOT NULL,"
            "y SMALLINT(4) NOT NULL,"
            "video_codec ENUM('h264','hevc','mpeg2video','mpeg4','msmpeg4v3','vc1','wmv2','wmv3','mpeg1video','flv1','msmpeg4v2','svq3','vp9','rv30','wmv1','mjpeg') NOT NULL,"
            "audio_codec ENUM('aac','ac3','dts','eac3','flac','mp2','mp3','opus','truehd','vorbis','wmav2','adpcm_ima_wav','pcm_s16le','adpcm_ima_qt','cook','wmapro','unknown') NOT NULL,"
            "audio_channels ENUM('1','2','5','6','7','8') NOT NULL,"
            "audio_layout ENUM('5.0','5.1','6.1','7.1','mono','stereo','unknown') NOT NULL,"
            "PRIMARY KEY (id))")
    db_conn.commit()
    print("Done")
    for curr_resolution in resolutions:
        print(f"Importing movies from {curr_resolution} ...")
        dir_videos = os.listdir(f"{root}/Movies/{curr_resolution}")
        video_dict = {}
        for file_name in dir_videos:
            file_size = os.path.getsize(f"{root}/Movies/{curr_resolution}/{file_name}")
            file_mtime = int(os.path.getmtime(f"{root}/Movies/{curr_resolution}/{file_name}"))
            real_res = str(subprocess.check_output(f"ffprobe -v quiet -select_streams v -show_entries stream=codec_name,width,height -of csv=p=0:s=, \"{root}/Movies/{curr_resolution}/{file_name}\" | head -n1", shell=True))
            real_res = real_res.split("'")
            real_res = real_res[1].replace("\\n", "")
            v_codec, x_val, y_val = real_res.split(",")
            audio = str(subprocess.check_output(f"ffprobe -v quiet -select_streams a -show_entries stream=codec_name,channels,channel_layout -of csv=p=0:s=, \"{root}/Movies/{curr_resolution}/{file_name}\" | head -n1", shell=True))
            audio = audio.split("'")
            audio = audio[1].replace("\\n", "")
            if audio == "":
                a_codec = "unknown"
                a_channels = 2
                a_layout = "unknown"
            else:
                a_codec, a_channels, a_layout = audio.split(",")
                a_layout = a_layout.replace("(side)", "")
            db.execute(f"INSERT INTO movies (name, size, mtime, res_folder, x, y, video_codec, audio_codec, audio_channels, audio_layout)"
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

    db.execute("ALTER TABLE movies ADD INDEX (res_folder)")
    db.execute("ALTER TABLE movies ADD INDEX (video_codec)")
    db.execute("ALTER TABLE movies ADD INDEX (audio_codec)")
    db.execute("ALTER TABLE movies ADD INDEX (audio_channels)")
    db.execute("ALTER TABLE movies ADD INDEX (audio_layout)")
    db_conn.commit()
    mysql_close()


def do_import_shows():
    mysql_open()
    print("Initializing tables ...")
    db.execute("CREATE OR REPLACE TABLE shows"
            "(id SMALLINT(4) UNSIGNED NOT NULL AUTO_INCREMENT,"
            "title VARCHAR(80) NOT NULL,"
            "PRIMARY KEY (id))")
    db.execute("CREATE OR REPLACE TABLE seasons"
            "(id SMALLINT(4) NOT NULL AUTO_INCREMENT,"
            "season SMALLINT(4) UNSIGNED NOT NULL,"
            "show_id SMALLINT(4) UNSIGNED NOT NULL,"
            "PRIMARY KEY (id))")
    db.execute("CREATE OR REPLACE TABLE episodes"
            "(id SMALLINT(5) UNSIGNED NOT NULL AUTO_INCREMENT,"
            "file_name VARCHAR(200) NOT NULL,"
            "show_id SMALLINT(3) NOT NULL,"
            "season_id SMALLINT(3) NOT NULL,"
            "size BIGINT(11) NOT NULL,"
            "mtime INT(10) NOT NULL,"
            "res ENUM('360p','480p','720p','1080p','4k'),"
            "x SMALLINT(4) UNSIGNED NOT NULL,"
            "y SMALLINT(4) UNSIGNED NOT NULL,"
            "video_codec ENUM('h264','hevc','mpeg2video','mpeg4','msmpeg4v3','vc1','wmv2','wmv3','mpeg1video','flv1','msmpeg4v2','svq3','vp9','rv30','wmv1','mjpeg') NOT NULL,"
            "audio_codec ENUM('aac','ac3','dts','eac3','flac','mp2','mp3','opus','truehd','vorbis','wmav2','adpcm_ima_wav','pcm_s16le','adpcm_ima_qt','cook','wmapro','unknown') NOT NULL,"
            "audio_channels ENUM('1','2','5','6','7','8') NOT NULL,"
            "audio_layout ENUM('5.0','5.1','6.1','7.1','mono','stereo','unknown') NOT NULL,"
            "PRIMARY KEY (id))")
    db_conn.commit()
    print("Done")
    print(f"Importing shows ...")
    dir_shows = os.listdir(f"{root}/TV")
    video_dict = {}
    for show_name in dir_shows:
        show_dir = os.listdir(f"{root}/TV/{show_name}")
        db.execute(f"INSERT INTO shows (title) VALUES ('{show_name}')")
        db_conn.commit()
        db.execute(f"SELECT id FROM shows WHERE title = '{show_name}'")
        show_id_var = db.fetchone()
        for dir_season in show_dir:
            print(f"Importing episodes from {show_name}/{dir_season} ...")
            season_num = dir_season.replace("Season ","")
            season_num = int(season_num)
            db.execute(f"INSERT INTO seasons (season, show_id) VALUES ('{season_num}', '{show_id_var[0]}')")
            db_conn.commit()
            db.execute(f"SELECT id FROM seasons WHERE season = '{season_num}' AND show_id = '{show_id_var[0]}'")
            season_id_var = db.fetchone()
            season_files = os.listdir(f"{root}/TV/{show_name}/{dir_season}")
            for file in season_files:
                file_safe = file.replace("'", "")
                file_safe = file_safe.replace("？", "")
                file_size = os.path.getsize(f"{root}/TV/{show_name}/{dir_season}/{file}")
                file_mtime = int(os.path.getmtime(f"{root}/TV/{show_name}/{dir_season}/{file}"))
                real_res = str(subprocess.check_output(f"ffprobe -v quiet -select_streams v -show_entries stream=codec_name,width,height -of csv=p=0:s=, \"/{root}/TV/{show_name}/{dir_season}/{file}\" | head -n1", shell=True))
                real_res = real_res.split("'")
                real_res = real_res[1].replace("\\n", "")
                v_codec, x_val, y_val = real_res.split(",")
                if int(y_val) <= 360:
                    common_res = "360p"
                elif int(y_val) <= 480:
                    common_res = "480p"
                elif int(y_val) <= 720:
                    common_res = "720p"
                elif int(y_val) <= 1080:
                    common_res = "1080p"
                else:
                    common_res = "4k"
                audio = str(subprocess.check_output(f"ffprobe -v quiet -select_streams a -show_entries stream=codec_name,channels,channel_layout -of csv=p=0:s=, \"{root}/TV/{show_name}/{dir_season}/{file}\" | head -n1", shell=True))
                audio = audio.split("'")
                audio = audio[1].replace("\\n", "")
                if audio == "":
                    a_codec = "unknown"
                    a_channels = 2
                    a_layout = "unknown"
                else:
                    a_codec, a_channels, a_layout = audio.split(",")
                    a_layout = a_layout.replace("(side)", "")
                    a_layout = a_layout.replace("(back)", "")
                db.execute(f"INSERT INTO episodes (file_name, show_id, season_id, size, mtime, res, x, y, video_codec, audio_codec, audio_channels, audio_layout) "
                        "VALUES ("
                        f"'{file_safe}',"
                        f"'{show_id_var[0]}',"
                        f"'{season_id_var[0]}',"
                        f"'{file_size}',"
                        f"'{file_mtime}',"
                        f"'{common_res}',"
                        f"'{x_val}',"
                        f"'{y_val}',"
                        f"'{v_codec}',"
                        f"'{a_codec}',"
                        f"'{a_channels}',"
                        f"'{a_layout}')")
            db_conn.commit()

    db.execute("ALTER TABLE seasons ADD INDEX (show_id)")
    db.execute("ALTER TABLE episodes ADD INDEX (show_id)")
    db.execute("ALTER TABLE episodes ADD INDEX (season_id)")
#    db.execute("ALTER TABLE videos ADD INDEX (audio_channels)")
#    db.execute("ALTER TABLE videos ADD INDEX (audio_layout)")
    db_conn.commit()
    mysql_close()


def do_search(search):
    mysql_open()
    print()
    db.execute(f"SELECT name, size, mtime, res_folder, x, y, audio_layout FROM movies WHERE name LIKE '%{search}%' ORDER BY name")
    movies_result = db.fetchall()
    db.execute("SELECT shows.title, COUNT(seasons.show_id) AS season_count "
            "FROM shows "
            "JOIN seasons ON shows.id = seasons.show_id "
            f"WHERE shows.title LIKE '%{search}%' "
            "GROUP BY shows.id "
            "ORDER BY title")
    shows_result= db.fetchall()
    mysql_close()
    #df = pd.DataFrame.from_records(db, columns=[i[0] for i in db.description])
    #print(df)
    print()
    print("Movies found:")
    for video in movies_result:
        res = str(video[3])
        res = res.strip("{}'")
        name = video[0]
        size = int(video[1] / 1024 / 1024)
        mtime = datetime.datetime.fromtimestamp(video[2])
        x = video[4]
        y = video[5]
        audio_layout = video[6]
        print(f"{name} | {res} | {size} MB | {mtime} | {x}x{y} | {audio_layout}")
    print()
    print()
    print("Shows found:")
    for show in shows_result:
        print(f"{show[0]} ({show[1]} seasons)")


def do_compare():
    problem_videos = []
    total_size = 0
    print()
    mysql_open()
    for curr_resolution in resolutions:
        dir_videos = os.listdir(f"{root}/{curr_resolution}")
        db.execute(f"SELECT name, mtime, size FROM movies WHERE res_folder = '{curr_resolution}'")
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
    elif response == "importmovies":
       do_import_movies()
    elif response == "importshows":
        do_import_shows()
    elif response == "s":
        print()
        do_search(input("Search string: "))
    elif response == "c":
        do_compare()

