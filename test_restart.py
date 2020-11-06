import os
import sys
import psutil
import logging

def restart_program():
    """Restarts the current program, with file objects and descriptors
       cleanup
    """
#p.get_open_files() +
    try:
        p = psutil.Process(os.getpid())
        for handler in p.connections():
            os.close(handler.fd)
    except Exception as e:
        logging.error(e)

    python = sys.executable
    os.execl(python, python, *sys.argv)

def main():

    while True:
        answer = input('Run more again? (y/n): ')

        if answer == 'n':
           print('Goodbye')
           break
        elif answer == 'y':
           restart_program()
        else:
            print('Invalid input.')

if __name__ == '__main__':
    main()
