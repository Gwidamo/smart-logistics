import logging
import sys

def setup_logging(name="LogisticsPipeline"):
    """Sets up a professional logging format for Spark jobs."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create a format: Time - Name - Level - Message
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console Handler (to see logs in Docker terminal)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(handler)
        
    return logger