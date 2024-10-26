# logging_config.py
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,  # Imposta il livello di logging desiderato
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Puoi aggiungere altri handler, formatter, ecc., se necessario