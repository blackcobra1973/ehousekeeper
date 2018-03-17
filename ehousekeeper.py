#!/usr/bin/python

import elasticsearch
import argparse
import time

class EHOUSE:
    def __init__(self, _args):
        self.args = _args
    def run(self):
        while True:
            try:
                es = elasticsearch.Elasticsearch(
                    ['localhost'],
                    sniff_on_start=True,
                    sniff_on_connection_fail=True,
                    sniffer_timeout=60
                )
            except:
                break
            query="clock:[0 TO %f]"%(time.time()-self.args.keep)
            for i in self.args.indexes:
                count = es.count(index=i,q=query)
                cc = count['count']/self.args.elements
                hits=es.search(
                    q=query,
                    index=i,
                    size=self.args.elements,
                    scroll='5m'
                )
                c = 0
                while True:
                    try:
                        scroll=es.scroll(scroll_id=hits['_scroll_id'], scroll='5m', )
                    except elasticsearch.exceptions.NotFoundError:
                        break
                    bulk = ""
                    for result in scroll['hits']['hits']:
                        c+=1
                        bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
                    cc -= 1
                    print "%s(%08d): %-8d of %-8d. %-8d left"%(i, cc, c,count['count'],count['count']-c),
                    stamp = time.time()
                    try:
                        es.bulk(bulk)
                    except:
                        print
                        break
                    print "%d seconds left"%(cc*(time.time()-stamp))
            es.transport.close()
            if not self.args.housekeeper:
                break
            time.sleep(self.args.run_every)

def main():
    parser = argparse.ArgumentParser(description='ElasticSearch Zabbix HouseKeeper')
    parser.add_argument('-N', '--elements', type=int, default=5000, help='Maximum number of elements per run')
    parser.add_argument('-H', '--housekeeper', action='store_true', help='Run HOUSEKEEPER in foreground')
    parser.add_argument('-R', '--run-every', type=int, default=600, help='Number of seconds between runs in housekeeper mode')
    parser.add_argument('-K', '--keep', type=int, default=604800, help='Keep --keep number of metrics (seconds) in HISTORY')
    parser.add_argument('indexes', metavar='NAME', type=str, nargs='+',
                    help='ElasticSearch indexes')
    _args = parser.parse_args()
    h = EHOUSE(_args)
    h.run()


if __name__ == '__main__':
    main()
