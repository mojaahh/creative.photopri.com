
import os
import ftplib
from dotenv import load_dotenv

def cleanup():
    load_dotenv()
    ftp_host = os.getenv('FTP_HOST')
    ftp_user = os.getenv('FTP_USER')
    ftp_pass = os.getenv('FTP_PASS')
    ftp_dir = os.getenv('FTP_DIR')

    try:
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_pass)
            ftp.cwd(ftp_dir)
            files_to_delete = ['debug_env.php', 'check_python_versions.php', 'find_python.php', 'check_libs.php', 'setup_server.php']
            for f in files_to_delete:
                try:
                    ftp.delete(f)
                    print(f"Deleted: {f}")
                except:
                    pass
    except Exception as e:
        print(f"Cleanup error: {e}")

if __name__ == "__main__":
    cleanup()
