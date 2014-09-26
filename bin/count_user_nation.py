import sys
import os
import smtplib
import email.MIMEMultipart
import email.MIMEText
import email.MIMEBase

mailto_list = ["liqiang@xingcloud.com"]
mail_host = "smtp.qq.com"
mail_user = "xamonitor@xingcloud.com"
mail_pass = "22C1NziwxZI5F"

def parse_ad(filename):
    ad = {}
    with open(filename) as f:
        for line in f:
            attr = line.strip().split("\t")
            key = attr[1] + "_" + attr[2]
            if not ad.has_key(key):
                ad[key] = {"hit":0,"miss":0,"total":0}
            if attr[3] == attr[4]:
                ad[key]["hit"] = ad[key]["hit"] +1
            elif not attr[4] == "\N":
                ad[key]["miss"] = ad[key]["miss"] +1
            ad[key]["total"] = ad[key]["total"] +1
    return ad

def parse_common_user(filename):
    users = {}
    with open(filename) as f:
        for line in f:
            attr = line.strip().split("\t")
            users[attr[0].lower()] = attr[1].lower()
    return users

def sendMail(subject,content, file1, file2):
    me="xamonitor@xingcloud.com"
    msg = email.MIMEMultipart.MIMEMultipart()
    msg['Subject'] = "user range " + subject
    msg['From'] = "liqiang@xingcloud.com"
    msg['To'] = ";".join(mailto_list)
    try:
        text_msg = email.MIMEText.MIMEText(content)
        msg.attach(text_msg)

        msg.attach(attach_file(file1))
        msg.attach(attach_file(file2))

        s = smtplib.SMTP()
        s.connect(mail_host)
        s.login(mail_user, mail_pass)
        s.sendmail(me, mailto_list, msg.as_string())
        s.close()
        return True
    except Exception, e:
        print str(e)
        return False

def attach_file(filename):
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)
    data = open(filename, 'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close()
    email.Encoders.encode_base64(file_msg)

    ## 设置附件头
    basename = os.path.basename(filename)
    file_msg.add_header('Content-Disposition','attachment', filename = basename)

    return file_msg

def analysis(day):
    ad_file = "/data1/user_attribute/nation/ad_all_log/" + day + ".log"
    ad_click_file = "/data1/user_attribute/nation/ad_click/" + day + ".log"
    game_file = "/data1/user_attribute/nation/gm_user_action/" + day + ".log"
    dmp_file = "/data1/user_attribute/nation/dmp_user_action/" + day + ".log"
    yac_file = "/data1/user_attribute/nation/yac_user_action/" + day + ".log"
    nav_file = "/data1/user_attribute/nation/nav_all/" + day + ".log"
    user_category_file = "/data0/log/user_category_result/pr/total/" + day + "/0.0"

    print "parse ad click..."
    ad_info = parse_ad(ad_click_file)

    print "parse ad..."
    ad_user = parse_common_user(ad_file)
    print "parse game..."
    game_user = parse_common_user(game_file)
    print "parse plugin..."
    plugin_user = parse_common_user(dmp_file)
    print "parse yac..."
    yac_user = parse_common_user(yac_file)
    print "parse nav..."
    nav_user = parse_common_user(nav_file)

    users = {"ad":ad_user,"337":game_user,"plugin":plugin_user,"yac":yac_user,"nav":nav_user}
    result = {}
    print "compare with category..."
    with open(user_category_file) as f:
        for line in f:
            attr = line.strip().split("\t")
            uid = attr[0].lower()

            for (p,user) in users.items():
                if uid in user:
                    key = p + "_" + user[uid]
                    if key not in result:
                        result[key] = {"cover":0, "total":0}
                    if not attr[1] == "0":
                        result[key]["cover"] = result[key]["cover"]  +1
                    result[key]["total"] = result[key]["total"]  +1

    print "write to file..."
    user_file_name = "/data1/user_attribute/nation/user_" + day + ".csv"
    user_file = open(user_file_name,"w")
    for k in sorted(result):
        v = result[k]
        pn = k.split("_")
        user_file.write("%s,%s,%s,%s"%(pn[0],pn[1],v["cover"]/2,v["total"]/2))
        user_file.write("\n")
    user_file.close()

    ad_file_name = "/data1/user_attribute/nation/ad_" + day + ".csv"
    ad_file = open(ad_file_name,"w")
    for k in sorted(ad_info):
        pn = k.split("_")
        v = ad_info[k]
        ad_file.write("%s,%s,%s,%s,%s"%(pn[0],pn[1],v["hit"],v["miss"],v["total"]))
        ad_file.write("\n")
    ad_file.close()

    sendMail(day, "result", user_file_name, ad_file_name)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        day = sys.argv[1]
    else:
        print "Wrong parameter : day"
        sys.exit(-1)

    analysis(day)