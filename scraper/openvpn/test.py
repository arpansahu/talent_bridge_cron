import subprocess
import time
import requests

def get_ip_address(ip_version='both'):
    try:
        if ip_version == 'both':
            ip_v4 = requests.get('https://ifconfig.me', params={"ip": "4"}).text.strip()
            ip_v6 = requests.get('https://ifconfig.me', params={"ip": "6"}).text.strip()
            return ip_v4, ip_v6
        elif ip_version == '4':
            ip = requests.get('https://ifconfig.me', params={"ip": "4"}).text.strip()
            return ip
        elif ip_version == '6':
            ip = requests.get('https://ifconfig.me', params={"ip": "6"}).text.strip()
            return ip
    except requests.RequestException as e:
        print(f"Failed to get IP address: {e}")
        return None

def connect_to_vpn(config_file):
    try:
        print("Connecting to VPN...")
        subprocess.run(['sudo', 'openvpn', '--config', config_file], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to connect to VPN: {e}")

def main():
    print("Checking IP address before VPN connection...")
    original_ip_v4 = get_ip_address('4')
    original_ip_v6 = get_ip_address('6')
    print(f"Original IPv4 address: {original_ip_v4}")
    print(f"Original IPv6 address: {original_ip_v6}")

    # Connect to VPN
    connect_to_vpn('openvpn.ovpn')

    # Wait a few seconds for the VPN connection to establish
    time.sleep(10)

    print("Checking IP address after VPN connection...")
    new_ip_v4 = get_ip_address('4')
    new_ip_v6 = get_ip_address('6')
    print(f"New IPv4 address: {new_ip_v4}")
    print(f"New IPv6 address: {new_ip_v6}")

if __name__ == "__main__":
    main()