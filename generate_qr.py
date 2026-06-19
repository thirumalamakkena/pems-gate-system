import qrcode
import os

users = [
    "USR001",
    "USR002",
    "USR003",
    "USR004",
    "USR005",
    "USR006",
    "USR007",
    "USR008",
    "USR009",
    "USR010",
    "USR011",
    "USR012",
    "USR013",
    "USR014",
    "USR015"
]

os.makedirs("qrcodes", exist_ok=True)

for user_id in users:
    qr = qrcode.QRCode(
        version=1,
        box_size=10,
        border=5
    )

    qr.add_data(user_id)
    qr.make(fit=True)

    img = qr.make_image()

    img.save(f"qrcodes/{user_id}.png")

print("QR codes generated successfully!")