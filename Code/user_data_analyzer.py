# Function to send reports
def sendReportEmail():
    """Based on From EMail ID, Password and To EMail ID from Config files,
        we are trying to send data from the gmail account.
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders
    import os.path

    from config import from_email, email_password, to_email

    email = from_email
    password = email_password
    send_to_email = to_email
    subject = 'Sending User reports'
    message = 'Please find the attachments with Users and Users by States, From Sushma Kesamsetty'
    file1_location = '../Reports/Users.csv'
    file2_location = '../Reports/UsersByState.csv'

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'plain'))

    # Setup the attachment
    filename = os.path.basename(file1_location)
    attachment = open(file1_location, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    # Attach the attachment to the MIMEMultipart object
    msg.attach(part)

    # Setup the attachment
    filename = os.path.basename(file2_location)
    attachment = open(file2_location, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    # Attach the attachment to the MIMEMultipart object
    msg.attach(part)

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    # server.starttls()
    server.login(from_email, password)
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()

    
# Initial Dependencies

import pandas as pd
from ast import literal_eval 

from config import dialect, username, password, host, port, database

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

#  DB Connection

connection_string = f"{dialect}://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)
connection = engine.connect()

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
stg_user_data = Base.classes.user_data_stg
user_data_tb = Base.classes.user_data

# Clear Staging table before every new load
session = Session(engine)
session.query(stg_user_data).delete()
session.commit()

# File path to load the data
user_file_path = "../Resources/sample_us_users.csv"
base_user_data_df = pd.read_csv(user_file_path)

# Load data into staging table
base_user_data_df.to_sql('user_data_stg',connection, if_exists='append', index=False)
session.commit()

# In reflection, the table will be visible only when it has primary key.. So I created a temporary key and dropping it here
stg_base_user_data_df = pd.read_sql_table('user_data_stg', connection)
stg_base_user_data_df = stg_base_user_data_df.drop(columns = ['index_col'])

# Convert the String Address to Dictionary
stg_base_user_data_df.iloc[:,1:2]=stg_base_user_data_df.iloc[:,1:2].applymap(literal_eval)

# Normalize the dictionary data
formatted_address = pd.json_normalize(stg_base_user_data_df.address)
formatted_address.columns = [f'address_{col}' for col in formatted_address.columns]

# Merge the Normalized address data with to the base data set
user_data_normalized_df = pd.concat([stg_base_user_data_df, formatted_address], axis = 1)

# Clean up the normalized data
formatted_user_data_df = user_data_normalized_df.drop(columns = ['address'])
formatted_user_data_df.rename(columns = {'address_postCode':'address_postcode'}, inplace = True)

# Clean up null values in the dataframe
nan_rows  = formatted_user_data_df[formatted_user_data_df.isna().any(axis=1)]
formatted_user_data_df.fillna({'address_state':'US'}, inplace=True)
cleansed_user_data_df = formatted_user_data_df[formatted_user_data_df.isna().any(axis=1)]

# Load cleansed data to the main table
formatted_user_data_df.to_sql('user_data',connection, if_exists='append', index=False)
session.commit()

# Read complete User data for reporting
db_user_data_df = pd.read_sql_table('user_data', connection)
db_user_data_df.drop(columns = ['index_col'], inplace = True)
db_user_data_df.set_index('id')

# User report
SQL_Statement = "SELECT ID, ADDRESS_CITY, ADDRESS_STATE, ADDRESS_COUNTRY, ADDRESS_POSTCODE \
                         FROM user_data "
UsersDF = pd.read_sql(SQL_Statement,connection)
UsersDF.to_csv('../Reports/Users.csv', index=False)

# User count by states
SQL_Statement = "SELECT ADDRESS_STATE, count(*) AS USER_COUNT \
                         FROM user_data \
                         GROUP BY ADDRESS_STATE \
                         ORDER BY USER_COUNT DESC"
UsersByStateDF = pd.read_sql(SQL_Statement,connection)
UsersByStateDF.to_csv('../Reports/UsersByState.csv', index=False)

# Send report in an email
sendReportEmail()

