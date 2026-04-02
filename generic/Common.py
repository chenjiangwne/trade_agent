import os
import traceback

import yaml
from loguru import logger


def yml_reader(yml_file=None):
    """
    Description: Read parameters defined in yml file configuration
    :param jfile, the yml file path
    :return dict
    """
    d_yml = dict()
    if not os.path.exists(yml_file):
        logger.error("Error! Invalid yml file assigned !")
        return d_yml

    with open(yml_file, "rb") as f_yml:
        cfg = f_yml.read()
        d_yml = yaml.load(cfg, Loader=yaml.SafeLoader)

    return d_yml


def get_traceback(comment=""):
    """
    Description: Get traceback info to error log.
    """
    tb_logs = []
    if comment.strip() != "":
        tb_logs.append("++ " + comment)
        tb_logs.append(" ")

    err_stack = traceback.extract_stack()
    err_stack = err_stack[5:]
    for err_frame in err_stack:
        tb_logs.append(str(err_frame))

    err = traceback.format_exc()
    err_lines = err.splitlines()
    tb_logs.append(" ")
    tb_logs += err_lines

    logger.debug("-" * 50)
    for err_ln in tb_logs:
        if "FrameSummary" in err_ln:
            continue
        logger.debug("|> " + err_ln)
    logger.debug("-" * 50)
