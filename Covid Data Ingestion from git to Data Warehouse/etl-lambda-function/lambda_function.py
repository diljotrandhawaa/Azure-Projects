import json
import psycopg2
import os

def lambda_handler(event, context):
    print("function started")
    print("event collected is {}".format(event))

    for record in event['Records'] :
        s3_bucket = record['s3']['bucket']['name']
        print("Bucket name is {}".format(s3_bucket))

        s3_key = record['s3']['object']['key']
        print("Bucket key name is {}".format(s3_key))

        from_path = "s3://{}/{}".format(s3_bucket, s3_key)
        print("from path {}".format(from_path))

        tablename = os.path.splitext(s3_key.split('/')[-1])[0]
        print(f"Extracted table name: {tablename}")

        # Access_key = os.getenv('AWS_Access_key')
        # Access_Secrete = os.getenv('AWS_Access_Secrete')
        dbname = os.getenv('dbname')
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')
        iam_role = os.getenv('iam_role')

        connection = psycopg2.connect(dbname = dbname,
                                       host = host,
                                       port = '5439',
                                       user = user,
                                       password = password)
                                       
        print('after connection....')
        curs = connection.cursor()
        print('after cursor....')

        # Dimhospital table creation
        curs.execute("""
            CREATE TABLE IF NOT EXISTS dimhospital (
            fips FLOAT,
            hospital_name VARCHAR(500),
            hospital_type VARCHAR(100),
            hq_address VARCHAR(200),
            hq_address1 VARCHAR(100),
            hq_city VARCHAR(100),
            hq_state CHAR(2),
            county_name VARCHAR(100),
            latitude DOUBLE PRECISION,
            longtitude DOUBLE PRECISION
            );
         """)

        #Dimregion table creation
        curs.execute("""
            CREATE TABLE IF NOT EXISTS dimregion (
                fips FLOAT,
                province_state VARCHAR(100),
                country_region VARCHAR(100),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                country VARCHAR(100)
            );
        """)

        #Dimdate table creation
        curs.execute("""
            CREATE TABLE IF NOT EXISTS dimdate (
                date TIMESTAMP,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                is_weekend BOOLEAN
            );
        """)

        #Factscovid table creation
        curs.execute("""
            CREATE TABLE IF NOT EXISTS factscovid (
                fips INTEGER,
                date INTEGER,
                state CHAR(2),
                positive DOUBLE PRECISION,
                negative DOUBLE PRECISION,
                pending DOUBLE PRECISION,
                hospitalized DOUBLE PRECISION,
                dateModified TIMESTAMP,
                recovered_x DOUBLE PRECISION,
                deathConfirmed DOUBLE PRECISION,
                hospitalizedDischarged DOUBLE PRECISION,
                active DOUBLE PRECISION,
                recovered_y DOUBLE PRECISION,
                confirmed DOUBLE PRECISION
            );
        """)


        query = f"""
        COPY {tablename}
        FROM '{from_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        REGION 'us-east-2';
        """

        print("query is {}".format(query))
        print('after querry....')
        curs.execute(query)

        connection.commit()
        #print(curs.fetchmany(3))
        print('after execute....')
        curs.close()
        print('after curs close....')
        connection.close()
        print('after connection close....')
        print('wow..executed....')
