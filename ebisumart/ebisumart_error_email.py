def sendEmail(body, subject,strTo,strMailSubject):
    import requests
    import json
    Mail_Key=dbutils.secrets.get(scope='storagescope',key='Azure-OcpApim-KeyValue')
    payload = json.dumps({
        "Email_Receipients": f"{strTo}",
        "Email_Subject": f"{strMailSubject} {subject}",
        "Email_Body": f"{body}",
        "Email_CC":'',
        "Email_Path":'<CSV path at blob location>',
        "Email_Attchmentname":'<excelfilename>'
    })
    headers = {
    'Content-Type': 'application/json',
    'Ocp-Apim-Subscription-Key': f'{Mail_Key}'
    }
    response = requests.request("POST", url_mail, headers=headers, data=payload)
    print("mail Response: ", response.text)
    if(response.text!=''):
        print(logs)

def