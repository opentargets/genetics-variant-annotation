#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import hail as hl

def main():

    ht = hl.read_table('data/gnomad.genomes.head100k.r2.1.sites.ht')
    ht.describe()
    ht.show()

    return 0

if __name__ == '__main__':

    main()
