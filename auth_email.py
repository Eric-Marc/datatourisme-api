#!/usr/bin/env python3
"""
Module d'authentification GEDEON
- Email + Pseudo + Mot de passe
- Confirmation par email via SendGrid
"""

import os
import secrets
import hashlib
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash

# SendGrid
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False
    print("⚠️ SendGrid non installé (pip install sendgrid)")

# Configuration
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
SENDGRID_FROM_EMAIL = os.environ.get('SENDGRID_FROM_EMAIL', 'noreply@gedeon.app')
APP_URL = os.environ.get('APP_URL', 'https://datatourisme-api-1.onrender.com')

# Durée de validité du token de confirmation (24h)
TOKEN_EXPIRY_HOURS = 24


def generate_confirmation_token():
    """Génère un token de confirmation unique (32 caractères hex)"""
    return secrets.token_hex(16)


def send_confirmation_email(email, pseudo, token):
    """
    Envoie l'email de confirmation via SendGrid.
    Retourne (success, error_message)
    """
    if not SENDGRID_AVAILABLE:
        return False, "SendGrid non disponible"
    
    if not SENDGRID_API_KEY:
        print("⚠️ SENDGRID_API_KEY non configuré")
        return False, "Configuration email manquante"
    
    confirmation_link = f"{APP_URL}/confirm?token={token}"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f9fafb; padding: 30px; border-radius: 0 0 10px 10px; }}
            .button {{ display: inline-block; background: #667eea; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; font-weight: bold; margin: 20px 0; }}
            .button:hover {{ background: #5568d3; }}
            .footer {{ text-align: center; color: #888; font-size: 12px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎉 Bienvenue sur GEDEON !</h1>
            </div>
            <div class="content">
                <p>Bonjour <strong>{pseudo}</strong>,</p>
                <p>Merci de vous être inscrit sur GEDEON, l'agenda global des événements.</p>
                <p>Pour activer votre compte, cliquez sur le bouton ci-dessous :</p>
                <p style="text-align: center;">
                    <a href="{confirmation_link}" class="button">✅ Confirmer mon compte</a>
                </p>
                <p>Ou copiez ce lien dans votre navigateur :</p>
                <p style="word-break: break-all; background: #e5e7eb; padding: 10px; border-radius: 5px; font-size: 12px;">
                    {confirmation_link}
                </p>
                <p><strong>Ce lien expire dans 24 heures.</strong></p>
            </div>
            <div class="footer">
                <p>GEDEON - L'agenda global des événements</p>
                <p>Si vous n'avez pas créé de compte, ignorez cet email.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    text_content = f"""
    Bienvenue sur GEDEON, {pseudo} !
    
    Pour confirmer votre compte, cliquez sur ce lien :
    {confirmation_link}
    
    Ce lien expire dans 24 heures.
    
    Si vous n'avez pas créé de compte, ignorez cet email.
    """
    
    message = Mail(
        from_email=SENDGRID_FROM_EMAIL,
        to_emails=email,
        subject='🎉 Confirmez votre compte GEDEON',
        html_content=html_content,
        plain_text_content=text_content
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        
        if response.status_code in [200, 201, 202]:
            print(f"📧 Email de confirmation envoyé à {email}")
            return True, None
        else:
            print(f"❌ SendGrid erreur: {response.status_code}")
            return False, f"Erreur d'envoi ({response.status_code})"
            
    except Exception as e:
        print(f"❌ SendGrid exception: {e}")
        return False, str(e)


def send_password_reset_email(email, pseudo, token):
    """
    Envoie l'email de réinitialisation de mot de passe.
    """
    if not SENDGRID_AVAILABLE or not SENDGRID_API_KEY:
        return False, "Configuration email manquante"
    
    reset_link = f"{APP_URL}/reset-password?token={token}"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background: #f59e0b; color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f9fafb; padding: 30px; border-radius: 0 0 10px 10px; }}
            .button {{ display: inline-block; background: #f59e0b; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; font-weight: bold; margin: 20px 0; }}
            .footer {{ text-align: center; color: #888; font-size: 12px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔐 Réinitialisation du mot de passe</h1>
            </div>
            <div class="content">
                <p>Bonjour <strong>{pseudo}</strong>,</p>
                <p>Vous avez demandé à réinitialiser votre mot de passe GEDEON.</p>
                <p style="text-align: center;">
                    <a href="{reset_link}" class="button">🔑 Changer mon mot de passe</a>
                </p>
                <p><strong>Ce lien expire dans 1 heure.</strong></p>
                <p>Si vous n'avez pas fait cette demande, ignorez cet email.</p>
            </div>
            <div class="footer">
                <p>GEDEON - L'agenda global des événements</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    message = Mail(
        from_email=SENDGRID_FROM_EMAIL,
        to_emails=email,
        subject='🔐 Réinitialisez votre mot de passe GEDEON',
        html_content=html_content
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code in [200, 201, 202], None
    except Exception as e:
        return False, str(e)


def is_valid_email(email):
    """Validation basique d'email"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def is_token_expired(created_at, hours=TOKEN_EXPIRY_HOURS):
    """Vérifie si un token a expiré"""
    if not created_at:
        return True
    expiry = created_at + timedelta(hours=hours)
    return datetime.now() > expiry
