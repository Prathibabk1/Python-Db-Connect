import logging
import os
import cloudstorage as gcs
import webapp2
import time
import csv
import cgi
import MySQLdb
import jinja2
from google.appengine.ext.webapp.util import run_wsgi_app



from google.appengine.api import app_identity

# Configure the Jinja2 environment.
JINJA_ENVIRONMENT = jinja2.Environment(
  loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
  autoescape=True,
  extensions=['jinja2.ext.autoescape'])

# Define your production Cloud SQL instance information.
_INSTANCE_NAME = 'my-project-cloud1:sample-db'



class MainPage(webapp2.RequestHandler):

  def get(self):
    bucket_name ='sample_bucket1'
    bucket = '/' + bucket_name
    filename =bucket+'/all_month.csv'

    if (os.getenv('SERVER_SOFTWARE') and os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
        db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='project2', user='root',charset='utf8')
    else:
        db = MySQLdb.connect(host='173.194.235.161', port=3306, db='project2', user='root', charset='utf8')
            # Alternatively, connect to a Google Cloud SQL instance using:
            # db = MySQLdb.connect(host='ip-address-of-google-cloud-sql-instance', port=3306, user='root', charset='utf 8')

    
    cursor = db.cursor()
    gcs_file = gcs.open(filename)
    line=csv.reader(gcs_file)
    count=0
    start=time.time()
    for data in line:
      if count == 0:
        count+=1
        pass
      cursor.execute('INSERT INTO earthquake (time1, latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,mag_id,updated,place,type_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',data)
    db.commit()
    self.response.write("---Time taken to insert data is %s seconds ---" % (time.time() - start))
    
    cursor.execute('''select week, count(mag2) as mag2, count(mag3) as mag3, count(mag4) as mag4, count(mag5) as mag5
                from
                    ((select
                        case
                                when date(time1) between cast('2015-05-19' as date) and cast('2015-05-25' as date) then 1
                                when date(time1) between cast('2015-05-26' as date) and cast('2015-06-01' as date) then 2
                                when date(time1) between cast('2015-06-02' as date) and cast('2015-06-08' as date) then 3
                                when date(time1) between cast('2015-06-09' as date) and cast('2015-06-15' as date) then 4
                                when date(time1) between cast('2015-06-16' as date) and cast('2015-06-22' as date) then 5
                                when date(time1) between cast('2015-06-23' as date) and cast('2015-06-28' as date) then 6
                            end week,
                            quakeid
                    from
                        earthquake) as week, (select
                        case
                                when mag between 2 and 2.99 then mag
                            end mag2,
                            quakeid
                    from
                        earthquake) as mag2, (select
                        case
                                when mag between 3 and 3.99 then mag
                            end mag3,
                            quakeid
                    from
                        earthquake) as mag3, (select
                        case
                                when mag between 4 and 4.99 then mag
                            end mag4,
                            quakeid
                    from
                        earthquake) as mag4, (select
                        case
                                when mag >= 5 then mag
                            end mag5,
                            quakeid
                    from
                        earthquake) as mag5)
                where
                    week.quakeid = mag2.quakeid and
                    week.quakeid = mag3.quakeid and
                    week.quakeid = mag4.quakeid and
                    week.quakeid = mag5.quakeid
                group by week''')

    guestlist = [];
    count=0
    for row in cursor.fetchall():
      if count==0:
        count+=1
        pass
      guestlist.append(dict([('Week',row[0]),('Between 2&3',row[1]),('Between 3&4',row[2]),('Between 4&5',row[3]),('Greater than 5',row[4])]))

    variables = {'guestlist': guestlist}
    template = JINJA_ENVIRONMENT.get_template('mainpage.html')
    self.response.write(template.render(variables))
    db.close()
    gcs_file.close()
    

app = webapp2.WSGIApplication([('/', MainPage)],
                              debug=True)
