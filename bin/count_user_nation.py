# -*- coding: utf-8 -*-

import sys
import os
import smtplib
import email.MIMEMultipart
import email.MIMEText
import email.MIMEBase

mailto_list = ["liqiang@xingcloud.com","yanglin@elex-tech.com","yinlong@elex-tech.com","chenshihua@elex-tech.com"]
mail_host = "smtp.qq.com"
mail_user = "xamonitor@xingcloud.com"
mail_pass = "22C1NziwxZI5F"

def parse_ad(filename):
    ad = {}
    with open(filename) as f:
        for line in f:
            attr = line.strip().split("\t")
            if not len(attr[2]) == 2:
                continue
            uid = attr[0].lower()
            if uid not in ad:
                ad[uid] = {"hit": 0, "miss": 0, "click": 0}
            if attr[3] == attr[4]:
                ad[uid]["hit"] = ad[uid]["hit"] +1
            elif not attr[4] == "\N":
                ad[uid]["miss"] = ad[uid]["miss"] +1
            ad[uid]["p"] = attr[1]
            ad[uid]["na"] = attr[2].lower()
            ad[uid]["click"] = ad[uid]["click"] +1
    return ad

def parse_common_user(filename, ad, pid=None):
    with open(filename) as f:
        for line in f:
            try:
                attr = line.strip().split("\t")
                uid = attr[0].lower()
                if uid not in ad:
                    if pid:
                        p = pid
                        na = attr[1].lower()
                    else:
                        p = attr[1]
                        na = attr[2].lower()

                    if len(na) == 2:
                        ad[uid] = {"hit": 0, "miss": 0, "click": 0, "p": p, "na": na}
            except Exception, e:
                print line


def sendMail(subject,content, filename):
    me="xamonitor@xingcloud.com"
    msg = email.MIMEMultipart.MIMEMultipart()
    msg['Subject'] = "user range " + subject
    msg['From'] = "liqiang@xingcloud.com"
    msg['To'] = ";".join(mailto_list)
    try:
        text_msg = email.MIMEText.MIMEText(content)
        msg.attach(text_msg)

        for f in filename:
            msg.attach(attach_file(f))

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

def countPN(key, value, usertype, collect):
    if key not in collect:
        collect[key] = {"hit": 0, "miss": 0, "click": 0, "cover": 0, "total": 0}
    collect[key]["hit"] = collect[key]["hit"] + value["hit"]
    collect[key]["miss"] = collect[key]["miss"] + value["miss"]
    collect[key]["click"] = collect[key]["click"] + value["click"]

    if usertype and not usertype == "0":
        collect[key]["cover"] = collect[key]["cover"] +1
    collect[key]["total"] = collect[key]["total"] + 1


def analysis(day):
    ad_file = "/data1/user_attribute/nation/ad_all_log/" + day + ".log"
    yac_file = "/data1/user_attribute/nation/yac_user_action/" + day + ".log"
    nav_file = "/data1/user_attribute/nation/nav_all/" + day + ".log"
    user_category_file = "/data0/log/user_category_result/pr/total/" + day + "/0.0"

    print "parse ad ..."
    ad_info = parse_ad(ad_file)
    # nav first
    print "parse nav..."
    parse_common_user(nav_file, ad_info)
    print "parse yac..."
    parse_common_user(yac_file, ad_info, "worlderror")

    nation_count = {}
    project_count = {}
    nation_project = {}

    print "compare with category..."
    user_category = {}
    with open(user_category_file) as f:
        for line in f:
            attr = line.strip().split("\t")
            if len(attr) == 2:
                user_category[attr[0].lower()] = attr[1]

    for (uid, v) in ad_info.items():
        na = ad_info[uid]["na"]
        p = ad_info[uid]["p"]
        union = p + "_" + na

        user_type = None
        if uid in user_category:
            user_type = user_category[uid]

        countPN(na, ad_info[uid], user_type, nation_count)
        countPN(p, ad_info[uid], user_type, project_count)
        countPN(union, ad_info[uid], user_type, nation_project)

    print project_count.keys()
    print "write to file..."
    result = {"nation": nation_count, "project": project_count, "nation_project": nation_project}
    filenames = []
    for (t, collect) in result.items():
        user_file_name = "/data1/user_attribute/nation/" + day + "_" + t + ".csv"
        filenames.append(user_file_name)
        user_file = open(user_file_name,"w")
        if "nation_project" == t:
            user_file.write("%s,%s,%s,%s,%s,%s,%s\n"%("project","nation","ad hit","ad miss","total ad click","cover user","total user"))
        else:
            user_file.write("%s,%s,%s,%s,%s,%s\n"%(t,"ad hit","ad miss","total ad click","cover user","total user"))
        for k in sorted(collect):
            v = collect[k]
            if "nation_project" == t:
                pn = k.split("_")
            #{"hit":0,"miss":0,"click":0,"cover":0,"total":0}
                user_file.write("%s,%s,%s,%s,%s,%s,%s\n"%(pn[0],pn[1],v["hit"],v["miss"],v["click"],v["cover"],v["total"]))
            else:
                user_file.write("%s,%s,%s,%s,%s,%s\n"%(k,v["hit"],v["miss"],v["click"],v["cover"],v["total"]))
        user_file.close()

    sendMail(day, "result", filenames)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        day = sys.argv[1]
    else:
        print "Wrong parameter : day"
        sys.exit(-1)

    analysis(day)