import config.server_cfg as config
import tableauserverclient as TSC
from datetime import datetime, timezone
import pandas as pd
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Global variables
INACTIVE_THRESHOLD = 500 # days

def tableau_signin(site_id):
    '''Sign in to Tableau server'''
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=config.TOKEN_NAME,
        personal_access_token=config.TOKEN_KEY,
        site_id=site_id
    )
    server = TSC.Server(config.SITE_URL, use_server_version=False)
    # server.add_http_options({'verify': False})
    server.auth.sign_in(tableau_auth)
    return server

def get_users(server):
    '''Get all the users from the Tableau server'''
    all_users = []
    all_users = list(TSC.Pager(server.users))
    print("Total number of users: ", len(all_users))
    return all_users

def get_sites(server):
    '''Get all the sites from the Tableau server'''
    sites_list =[]
    all_sites = list(TSC.Pager(server.users))
    for site in all_sites:
        sites_list.append(site.content_url)
    print("Total number of sites: ", len(sites_list))
    server.auth.sign_out()
    return sites_list

def get_inactive_users(user_list, site_name):
    '''Identify viewers whose last sign in > INACTIVE_THRESHOLD days : Inactive users'''
    now = datetime.now(timezone.utc)
    inactive_users = []

    # Filter users with the Viewer role and check their last login date
    viewer_users = [user for user in user_list if user.site_role == "Viewer"]
    print(f"Total number of users with Site role as Viewer: {len(viewer_users)}")

    for user in viewer_users:
        if user.last_login is None or (now - user.last_login).days > INACTIVE_THRESHOLD:
            days_inactive = (now - user.last_login).days if user.last_login else "Never logged in"
            inactive_users.append(user)
    print("Total number of inactive users: ", len(inactive_users))

    # Save the inactive user info to a CSV file
    print("Saving the inactive user info to a csv file...")
    save_info(inactive_users, site_name)

    return inactive_users
def save_info(inactive_users, site_name):
    '''Save the inactive user info to a csv file'''
    parsed_users_info = []
    for user in inactive_users:
        user_info = {
            "Name": user.name,
            "Full Name": user.fullname,
            "Role": user.site_role,
            "Last Login": user.last_login,
            "Email": user.email,
            "ID": user.id
        }
        parsed_users_info.append(user_info)

    df = pd.DataFrame(parsed_users_info)

    today = datetime.now().strftime("%Y-%m-%d %H-%M")
    folder_path = os.path.join(config.SAVE_LOCATION, today)

    # Create the folder if it does not exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    csv_filename = f"{site_name}_users_info.csv"
    csv_path = os.path.join(folder_path, csv_filename)

    df.to_csv(csv_path, index=False)
    print(f"User information saved to {csv_path}")

    return csv_path

def deactivate_users(inactive_users):
    '''Deactivate the inactive users'''

    if len(inactive_users) == 0:
        print("No inactive users to deactivate.")
        return

    for user in inactive_users:
        print(f"Deactivating user: {user.name} ...", end="")
        user.site_role = "Unlicensed"
        # if user.auth_setting != user.Auth.TableauIDWithMFA:
        #     user.auth_setting = user.Auth.TableauIDWithMFA
        server.users.update(user)
        print("\tdone!")
    print("Deactivation of users completed.")

def disable_all_users_group(server):
    '''Unset the grantLicenseMode for all user groups'''
    all_groups, pagination_item = server.groups.get()
    for group in all_groups:
        if group.name == "All Users":
            print(f"Unsetting license mode for group: {group.name}")
            group.LicenseMode = "unset"
            server.groups.update(group)

def enable_all_users_group(server):
    '''Activate the grantLicenseMode for all user groups'''
    all_groups, pagination_item = server.groups.get()
    for group in all_groups:
        if group.name == "All Users":
            print(f"Activating license mode for group: {group.name}")
            group.LicenseMode = "onLogin"  # Or whatever mode you need to set
            group.minimum_site_role = 'Viewer'  # Adjust this to the desired role
            server.groups.update(group)

def send_email(subject, body, attachments):
    '''Send an email with the specified subject, body, and attachments'''
    msg = MIMEMultipart()
    msg['From'] = config.EMAIL_SENDER
    msg['To'] = config.EMAIL_RECEIVER
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    for filename in attachments:
        attachment = open(filename, 'rb')

        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= " + filename)

        msg.attach(part)

    server = smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT)
    server.starttls()
    server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
    text = msg.as_string()
    server.sendmail(config.EMAIL_SENDER, config.EMAIL_RECEIVER, text)
    server.quit()

if __name__ == "__main__":
    print("--**--")
    try:

        sites_list = ['b360bi']

        # server = tableau_signin("") #TODO: Site list
        # sites_list = get_sites(server)

        for site_list in sites_list:
            print(f"Processing site: {site_list}")
            # Sign in to Tableau server
            server = tableau_signin(site_list)

            try:
                # Disable minimum site role before processing users
                disable_all_users_group(server)

                # Get all the users from the Tableau server
                user_list = get_users(server)

                # Identify inactive users with Viewer Site Role
                inactive_users = get_inactive_users(user_list, site_list)

                # Deactivate the inactive users
                #deactivate_users(inactive_users)

                # TASK 5:Re-enable minimum site role after processing user
                enable_all_users_group(server)

            except Exception as e:
                print(f"ERROR: Failed to process site '{site_list}' - {e}")
            finally:
                server.auth.sign_out()
                print(f"Successfully signed out of Tableau server for site '{site_list}'.")

    except Exception as e:
        print("ERROR: ", e)
    finally:
        print("--**--")