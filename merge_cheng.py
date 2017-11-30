#! /usr/bin/python
import re, os, glob, time
import sys
import argparse
from shutil import copyfile

log = list()
re_log = list()
indir = ''
outfile = ''
delimiter=''
dumporig = False

debug = True

def dprint(msg):
    global debug
    if debug:
        print("[INFO] "+msg)

def m_assert(cond, msg):
    if not cond:
        print("FATAL ERROR: " + msg + "\n")
        exit(1)

def byfirstindex(t):
    return t[0]

# assumption: the log is sorted
def rearrange_txn():
    global log, re_log
    rid_txn_ops = {}
    rid_in_txn = {}

    begin_pattern = re.compile('begin', re.I)
    commit_pattern = re.compile('commit', re.I)

    prev_rid = -1 
    # 1#&#3000#&#write#&#true#&#SET group_concat_max_len = 262144, sql_mode = ''
    dprint("start rearrange_txn")
    for indx, entry in log:

        # this is the end of one rid; rid follows prev_rid
        if entry == "REQ_END":
            dprint("met REQ_END with indx="+str(indx) + " for rid="+str(prev_rid))
            m_assert(prev_rid!=-1, "start with REQ_END or double REQ_END")
            # if there is implicit-commit txn, commit it!
            if (prev_rid in rid_in_txn) and rid_in_txn[prev_rid]==True :
                # (1) add all the entries in txn into log 
                # (2) clear the current txn and rid_in_txn
                # (3) set prev_rid=0
                for op in rid_txn_ops[prev_rid]:
                    re_log.append(op)
                rid_in_txn[rid] = False
                rid_txn_ops[rid] = list() 
                prev_rid = -1 
            # skip no matter what
            continue

        words = entry.split("#&#")
        prev_rid = rid = int(words[0])
        sql = words[4]

        # if begin 
        if re.match(begin_pattern, sql):
            dprint("met BEGIN with indx="+str(indx) + " for rid="+str(rid))
            # No nested txn: either not in rid_in_txn or rid_in_txn[rid]==False
            m_assert( not (rid in rid_in_txn) or rid_in_txn[rid]==False, "begin fail, " + entry)
            # add entry to this
            rid_in_txn[rid] = True
            rid_txn_ops[rid] = [entry] 
            # protection check
            m_assert( int(words[1]) % 1000 == 0, "begin opnum isn't X000")

        # if commit
        elif re.match(commit_pattern, sql):
            dprint("met COMMIT with indx="+str(indx) + " for rid="+str(rid))
            # must within one txn
            m_assert( (rid in rid_in_txn) and rid_in_txn[rid] == True, "commit fail, " + entry)
            rid_txn_ops[rid].append(entry)
            # (1) add all the entries in txn into log 
            # (2) clear the current txn and rid_in_txn
            for op in rid_txn_ops[rid]:
                re_log.append(op)
            rid_in_txn[rid] = False
            rid_txn_ops[rid] = list() 

        else: # other ops (non-begin/commit)
            dprint("met op with indx="+str(indx) + " for rid="+str(rid))
            # if current rid is in txn
            if (rid in rid_in_txn) and rid_in_txn[rid]==True:
                rid_txn_ops[rid].append(entry)
            # if current rid is a single op
            else:
                re_log.append(entry)

    # final check: (1) no active txn, (2) nothing in txn op
    for rid in rid_in_txn:
        m_assert(not rid_in_txn[rid], "rid[" + str(rid) + "] is till in txn")
    for rid in rid_txn_ops:
        m_assert(len(rid_txn_ops[rid]) == 0, "rid[" + str(rid) + "] has ops!")


# assumption: the log is rearranged
def dump_to_file():
    global re_log, outfile, delimiter

    with open(outfile, 'wb') as f:
        for op in re_log:
            f.write((op + delimiter).encode('latin1'))
        f.write('END|]|')

def dump_origlog_to_file():
    global log, outfile, delimiter

    with open(outfile, 'wb') as f:
        for indx, op in log:
            dprint("meet op index=" + str(indx));
            if indx.is_integer():
                f.write((op + delimiter).encode('latin1'))
        f.write('END|]|')

def CheckAndMerge():
    global log, indir, dumporig

    for filename in glob.glob(os.path.join(indir, '*.log')):

        prev_indx = 0
        with open(filename, 'rb') as f:
            s = f.read().decode('latin1')
            res = re.findall(r'\|\[\|(.*?)::(.*?)\|\]\|', s, re.S)
            for v in res:
                log.append((float(v[0]), v[1]))
                prev_indx = float(v[0])

        m_assert( s[-3:] == 'END', "the file " + filename + " is not ended!" )
        os.rename(filename, filename+'.bak')
        # in order to handle implicit commit
        log.append( (prev_indx+0.5, "REQ_END") )

    if len(log)>0:
        log = sorted(log, key = byfirstindex)
        if dumporig:
            dump_origlog_to_file()
        else:
            rearrange_txn()
            dump_to_file()

def main(argv):
    global indir, outfile, dumporig, delimiter

    parser = argparse.ArgumentParser(description = 'MySQL Log Merger')
    parser.add_argument('--indir', type=str, default='/tmp/sql_log/', help = 'path of input directory (default /tmp/sql_log/)')
    parser.add_argument('--output', type=str, default='/tmp/sql.log', metavar='PATH', help='output merged file path (default /tmp/sql.log)')
    parser.add_argument('--delimiter', type=str, default='|]|', help='delimiter for entries (default |]|)')
    parser.add_argument('--revert', action='store_true', default = False, help='rename all *.log.bak back to *.log')
    parser.add_argument('--dumporig', action='store_true', default = False, help='dump the log with original sequence')

    args = parser.parse_args()

    indir = args.indir
    outfile = args.output
    delimiter = args.delimiter
    # special case
    if delimiter == "newline":
        delimiter = "\n"
    dumporig = args.dumporig

    if args.revert:
        for filename in glob.glob(os.path.join(indir, '*.log.bak')):
            os.rename(filename, filename[:-4])
        exit(0)

    if os.path.isfile(args.output):
        os.remove(args.output)

    CheckAndMerge()

if __name__ == "__main__":
    main(sys.argv)
