import os 

#sudo nano /etc/myapp.env
#"MYSQL_DSA_HOST"="10.100.100.61"
#"MYSQL_DSA_PORT"="32817"
#"MYSQL_DSA_USER"="dsa_write_test"
#"MYSQL_DSA_PASS"="dsaWRITE?mySQL!TEST$""


def pwd (key):

    mapping = {


        "mysql_dsa_host":"MYSQL_DSA_HOST",
        "mysql_dsa_port":"MYSQL_DSA_PORT",
        "mysql_dsa_user":"MYSQL_DSA_USER",
        "mysql_dsa_pass":"MYSQL_DSA_PASS"




    }

    return os.getenv(mapping.get(key,""),"")
