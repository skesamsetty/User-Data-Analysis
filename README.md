# User-Data-Analysis

User Data Analysis using Python on Postgres using SQLAlchemy

This App expects to provide "config.py" in the folder "Code" with below information.

If i had more time, I could have provided AWS RDS so validated could directly access table contents.

    dialect='postgresql'
    username='postgres'
    password='DBPassword'
    host = "localhost"
    port = "5432"
    database = "BC_UserAnalyzer"
    from_email = 'from_email@gmail.com'
    email_password = 'gmail_password'
    to_email = 'to_email@gmail.com'

I have not worked on Apache Airflow (per the job description), I could do it in Apache Nifi and would need some more time to complete it.