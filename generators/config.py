# -*- coding: utf-8 -*-
# !/usr/bin/env python

import os
import yaml


def load_build_config():
    filePath = os.path.dirname(__file__)
    # print(filePath)
    yaml_path = os.path.join(filePath, '../build_config.yaml')
    with open(yaml_path, 'r') as f:
        # build_config = yaml.load(f) 方法已过时
        build_config = yaml.safe_load(f)
    return build_config


if __name__ == "__main__":
    config = load_build_config()
    print(config)