import pandas as pd
import tableauserverclient as TSC
import config.server_cfg as config
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def tableau_signin():
    '''Sign in to Tableau server'''
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=config.TOKEN_NAME,
        personal_access_token=config.TOKEN_KEY,
        site_id=config.SITE_NAME
    )
    server = TSC.Server(config.SITE_URL, use_server_version=True)
    server.auth.sign_in(tableau_auth)

    return server

def add_users(server, csv_file_path):
    '''Add users to Tableau server from a CSV file'''
    # Read the CSV file
    user_data = pd.read_csv(csv_file_path)

    # print("CSV Columns:", user_data.columns)

    # Get all users on the server
    all_users, pagination_item = server.users.get()

    # Iterate over the rows of the DataFrame
    for index, row in user_data.iterrows():
        # Check if the user already exists
        user_exists = any(u.name == row['username'] for u in all_users)
        if user_exists:
            print(f"User {row['username']} already exists. Skipping.")
            continue

        # Create a UserItem object with the required details
        new_user = TSC.UserItem(
            name=row['username'],
            site_role=row['site_role'],
            auth_setting=row['auth_setting']
        )

        # Add the user to the Tableau Server
        try:
            server.users.add(new_user)
            print(f"Successfully added user: {row['username']}")
        except TSC.ServerResponseError as e:
            print(f"Failed to add user: {row['username']} - {e}")

def send_email(subject, body):
    '''Send an email notification'''
    try:
        # Email configuration
        smtp_server = config.SMTP_SERVER
        smtp_port = config.SMTP_PORT
        smtp_user = config.SMTP_USER
        smtp_password = config.SMTP_PASSWORD
        from_email = config.EMAIL_SENDER
        to_email = config.EMAIL_RECEIVER

        # Create the email
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        # Send the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
        print(f"Email sent: {subject}")
    except Exception as e:
        print(f"Failed to send email: {e}")

def get_users(server):
    '''Fetch all users from Tableau server'''
    all_users = []
    response = server.users.get()
    all_users.extend(response[0])  # Extract users from the response
    return all_users

def update_users(server, csv_file_path):
    '''Update users on Tableau server from a CSV file'''
    # Read the CSV file
    user_data = pd.read_csv(csv_file_path)

    # Fetch all users from Tableau Server
    all_users = get_users(server)

    # Create a dictionary for quick lookup based on username
    user_dict = {user.name: user for user in all_users}

    # Iterate over the rows of the DataFrame
    for index, row in user_data.iterrows():
        try:
            username = row.get('username')
            site_role = row.get('site_role')
            fullname = row.get('fullname')
            email = row.get('email')

            # Find the user object from Tableau Server
            user = user_dict.get(username)
            if user is None:
                print(f"User {username} not found.")
                continue

            # Update user details
            if fullname:
                user.fullname = fullname
            if email:
                user.email = email
            if site_role:
                if site_role:
                    if site_role.lower() == 'unlicensed':
                        server.users.remove(user.id)
                        print(f"Successfully changed user {username} to Unlicensed")
                        continue
                    else:
                        user.site_role = site_role

            # Update the user on Tableau Server
            updated_user = server.users.update(user)
            print(f"Successfully updated user: {username} |  site role : {updated_user.site_role} | full name: {updated_user.fullname} | email: {updated_user.email}")
        except TSC.ServerResponseError as e:
            print(f"Failed to update user: {username} - {e}")
        except Exception as e:
            print(f"Unexpected error occurred while updating user: {username} - {e}")


# Example usage
if __name__ == "__main__":
    # Sign in to Tableau server
    print("--**--")
    try:
        server = tableau_signin()
        print("INFO: Successfully signed in to Tableau server.")
    except Exception as e:
        print("Could not sign in to Tableau server !")
        send_email("Tableau User Management Script Failed", f"Could not sign in to Tableau server!\nError: {e}")
        print("ERROR: ", e)
        exit(1)
    print("")

    csv_file_path = config.CSV_FILE_PATH

    ## Adding users
    add_users(server, csv_file_path)


    # Updating users
    update_users(server, csv_file_path) #TODO: Uncomment to update userinfo

    server.auth.sign_out()
    print("INFO: Signed out from Tableau server.")

    # Send completion email
    email_subject = "Tableau User Management Script Completed"
    email_body = "The Tableau user management script has been successfully executed."
    # send_email(email_subject, email_body)