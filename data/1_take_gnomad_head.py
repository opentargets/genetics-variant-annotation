#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import hail as hl

def main():

    (
      hl.read_table('gs://gnomad-public/release/2.1/ht/genomes/gnomad.genomes.r2.1.sites.ht')
        .head(100000)
        .write('gs://genetics-portal-analysis/gnomad.genomes.head100k.r2.1.sites.ht')
    )

    return 0

if __name__ == '__main__':

    main()
