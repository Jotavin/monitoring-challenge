import os
from dotenv import load_dotenv
from twilio.rest import Client

load_dotenv()


def check_alert_conditions(data, time):
    alerts = []
    for item in data:
        denied = item.get('denied', 0)
        reversed = item.get('reversed', 0)
        failed = item.get('failed', 0)
        
        if denied >= 70.5:
            alert_message = f"Alerta às {time}: 'denied' atingiu {denied} (>= 70.5)"
            alerts.append(alert_message)
            send_sms_alert(alert_message)  # Enviar SMS
        if reversed >= 7.5:
            alert_message = f"Alerta às {time}: 'reversed' atingiu {reversed} (>= 7.5)"
            alerts.append(alert_message)
            send_sms_alert(alert_message)  # Enviar SMS
        if failed > 0:
            alert_message = f"Alerta às {time}: 'failed' é {failed} (> 0)"
            alerts.append(alert_message)
            send_sms_alert(alert_message)  # Enviar SMS

        

    
    return alerts

def send_sms_alert(message):
    # Carregar as credenciais do Twilio
    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    twilio_number = os.getenv('TWILIO_PHONE_NUMBER')
    destination_number = os.getenv('DESTINATION_PHONE_NUMBER')

    client = Client(account_sid, auth_token)

    # Enviar a mensagem SMS
    try:
        message = client.messages.create(
            body=message,
            from_=twilio_number,
            to=destination_number
        )
        print(f"Alerta SMS enviado com sucesso! SID: {message.sid}")
    except Exception as e:
        print(f"Erro ao enviar SMS: {e}")

# Exemplo de uso
send_sms_alert("Alerta de Transações: denied atingiu 70.5") 