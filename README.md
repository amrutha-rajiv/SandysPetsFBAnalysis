SandysPetsFBAnalysis
====================


<b>FBtoSQLite.py</b> - Pulls Facebook Data and stores it in a SQLite database
This program can pull Albums & Photos and the Facebook Page's Timeline feed.

To collect the page's <b>Timeline Feed<b>, call the <i>getPageFeed</i> function
<i>getPageFeed(page_name,DB_NAME)</i>

To collect the page's <b>Album & photo information</b>, call <i>insertAlbumsAndPhotosInDB.</i>
<i>insertAlbumsAndPhotosInDB(page_name,DB_NAME)</i>

<i>page_name</i> for the facebook page with URL https://www.facebook.com/sandyspets would be sandyspets

<i>DB_NAME</i> is the name of your SQLite Database that you want to store the Data in.

If the Database tables do not exist, FBtSQLite will create the tables.

<b>Note:</b> The Facebook Access Token is valid only for two hours, if the program stops executing after 2 hours, it is likely that the access token has expired. Restarting the program will resume pulling of data. 

Requirements:
python2.7
sqlite3
pip

To install additional requirements please run the command pip install -r requirements.txt	