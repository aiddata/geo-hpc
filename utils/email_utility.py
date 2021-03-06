import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


class GeoEmail():
    """used for sending emails within geo framework
    """
    def __init__(self, config):
        self.config = config
        self.client = config.client
        self.c_email = config.client.asdf.email
        self.branch = config.branch

        self.defaults = {
            'reply_to': 'AidData GeoQuery <geo@aiddata.wm.edu>',
            'sender': 'noreply@aiddata.wm.edu'
        }


    def send_email(self, receiver, subject, message, sender=None, reply_to=None, passwd=None):
        """send an email

        Args:
            receiver (str): email address to send to
            subject (str): subject of email
            message (str): body of email

        Returns:
            (tuple): status, error message, exception
            status is bool
            error message and exception are None on success
        """
        receiver_list = [i.strip() for i in receiver.split(",")]
        receiver_str = ", ".join(receiver_list)

        if sender is None:
            sender = self.defaults['sender']
        if reply_to is None:
            reply_to = self.defaults['reply_to']
        if passwd is None:
            try:
                pw_search = self.c_email.find({"address": sender},
                                              {"password":1})

                if pw_search.count() > 0:
                    passwd = str(pw_search[0]["password"])
                else:
                    msg = "Error - specified email does not exist"
                    return 0, msg, Exception(msg)

            except Exception as e:
                return 0, "Error looking up email", e

        try:
            # source:
            # http://stackoverflow.com/questions/64505/
            #   sending-mail-from-python-using-smtp

            msg = MIMEMultipart()

            msg.add_header('reply-to', reply_to)
            msg['From'] = reply_to
            msg['To'] = receiver_str
            msg['Subject'] = subject
            msg.attach(MIMEText(message))

            mailserver = smtplib.SMTP('smtp.gmail.com', 587)
            # identify ourselves to smtp gmail client
            mailserver.ehlo()
            # secure our email with tls encryption
            mailserver.starttls()
            # re-identify ourselves as an encrypted connection
            mailserver.ehlo()

            mailserver.login(sender, passwd)
            mailserver.sendmail(sender, receiver_list, msg.as_string())
            mailserver.quit()

            return 1, None, None

        except Exception as e:
            return 0, "Error sending email", e


    def send_backup_email(self, receiver, subject, message, sender=None, reply_to=None, passwd=None):
        """send an email using alternative method

        Args:
            receiver (str): email address to send to
            subject (str): subject of email
            message (str): body of email

        Returns:
            (tuple): status, error message, exception
            status is bool
            error message and exception are None on success
        """

        if sender is None:
            sender = self.defaults['sender']
        if reply_to is None:
            reply_to = self.defaults['reply_to']

        try:

            # source:
            # http://effbot.org/pyfaq/how-do-i-send-mail-from-a-python-script.htm
            FROM = reply_to

            MAIN = [receiver]
            CC = []
            BCC = []
            # who it is going to, main, cc, bcc
            # must be a list
            TO = MAIN + CC +  BCC

            # Prepare actual message
            message = """\
            From: %s
            To: %s
            CC: %s
            Subject: %s

            %s
            """ % (FROM, ', '.join(MAIN), ', '.join(CC), subject, message)

            # Send the mail
            SERVER = "localhost"
            server = smtplib.SMTP(SERVER)
            server.sendmail(FROM, TO, message)
            server.quit()

            return 1, None, None

        except Exception as e:
            return 0, "Error sending backup email", e
