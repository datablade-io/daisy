#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import os
import argparse


class Image:
    def __init__(self, name, path, dependent):
        self.name = name
        self.path = path
        self.dependent = dependent


def build_dependency(f):
    with open(f) as fd:
        dp = json.load(fd)
    images = []
    built_image = set()

    for k, v in dp.items():
        images.append(Image(v['name'], k, v['dependent']))

    while len(images) > 0:
        batch = []
        length = len(images)
        for i in range(len(images)):
            node = images.pop(0)
            if len(node.dependent) == 0 or all([deps in built_image for deps in node.dependent]):
                batch.append(node)
                built_image.add(node.path)
                continue
            images.append(node)
        assert (len(images) != length)
        assert (len(batch) != 0)
        build_batch(batch)


def build_batch(nodes):
    pl = dict()
    for n in nodes:
        cmd = 'docker build -t ' + n.name + ' .'
        print("[" + n.path + "]: start executing " + cmd)
        pl[n.name] = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=n.path, encoding='utf-8')
        os.set_blocking(pl[n.name].stdout.fileno(), False)
        os.set_blocking(pl[n.name].stderr.fileno(), False)
    while any([p.poll() is None for _, p in pl.items()]):
        for k, v in pl.items():
            out = v.stdout.readlines()
            for line in out:
                try:
                    print("[" + k + "]: " + line.strip())
                except UnicodeEncodeError as e:
                    print(e)
            err = v.stderr.readlines()
            for line in err:
                try:
                    print("[" + k + "]: " + line.strip())
                except UnicodeEncodeError as e:
                    print(e)

    if any([p.poll() is not None and p.poll() != 0 for _, p in pl.items()]):
        print("build docker images failed")
        exit(0)


def main():
    parser = argparse.ArgumentParser(description='Build docker image')
    parser.add_argument('--path', dest='path', default=os.getcwd(), help='The root directory of daisy')
    args = parser.parse_args()

    build_dependency(args.path + '/docker/images.json')
    exit(0)


if __name__ == "__main__":
    main()
