#/usr/local/bin/python3
import sys, os

def progress(iteration, total, decimals=1, length=100):
    """
    Displays a nice progress bar in the terminal
    """
    prefix, suffix   = '',''
    formatStr       = "{0:." + str(decimals) + "f}"
    percents        = formatStr.format(100 * (iteration / float(total)))
    filledLength    = int(round(length * iteration / float(total)))
    bar             = '█' * filledLength + '-' * (length - filledLength)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),
    if iteration == total: sys.stdout.write('\n')
    sys.stdout.flush()

def bisearch(Gneib, source, target):
    """
    Performs a bidirectional search in the neibhor list array Gneib
    to find the order of connection between the two users 'source' and 'target'.
    The code is based on the shortest_path algorithm for network graphs.
    """

    # the problem statement is unclear if the transaction should be always
    # trusted if id1 = id2. For now we treat this case like any other.
    # if target == source:
    #     return 0

    if not source in Gneib or not target in Gneib:
        return 5
    # predecesssor and successors in search
    pred=set([source])
    succ=set([target])

    # initialize fringes, start with forward
    forward_fringe=set([source])
    reverse_fringe=set([target])

    # Below, we search in the neighbor list of source and target to find a
    # common friend. A forward fringe starts from the source and a reverse fringe
    # starts from the target and stores all users within the fringe. A path is found
    # when the fringes meet. In the present case, we stop if the sum of fringe lengths
    # (number of intermediate nodes) is greater than 4.

    # This method is easily extensible to higher order neighbors
    neibs = 0
    while forward_fringe and reverse_fringe:
        neibs += 1
        if neibs>4: return 5
        if len(forward_fringe) <= len(reverse_fringe):
            this_level=forward_fringe
            forward_fringe=set()
            for v in this_level:
                wset = Gneib[v].difference(pred)
                if wset.intersection(succ):  return neibs
                forward_fringe.update(wset)
                pred.update(wset)
        else:
            this_level=reverse_fringe
            reverse_fringe=set()
            for v in this_level:
                wset = Gneib[v].difference(succ)
                if wset.intersection(pred):  return neibs
                reverse_fringe.update(wset)
                succ.update(wset)

def neibadd(id1,id2):
    """
    Build the neighbor list. For each user, a list of unique users
    with transaction history is saved.
    """
    if id1 in Gneib: Gneib[id1].add(id2)
    else: Gneib[id1] = set([id2])
    if id2 in Gneib: Gneib[id2].add(id1)
    else: Gneib[id2] = set([id1])

fbatch, fstream = sys.argv[1], sys.argv[2]
fout = [sys.argv[3+x] for x in range(3)]
cwd = os.getcwd() + '/'

Gneib = {}

print('building payment graph...')
nlines = sum(1 for line in open(cwd+fbatch,'r'))-1
if not nlines: sys.exit()
with open(cwd+fbatch,'r') as bpay:
    next(bpay)          # ignore header line
    for cnt,line in enumerate(bpay):
        data = line.split(',')
        # check if the record is valid: starts with a date-time stamp.
        if ''.join(data[0][x] for x in [4,7,13,16])=='--::':
            if len(data)>3:
                id1, id2 = data[1].strip(), data[2].strip()
                neibadd(id1,id2)
                if not cnt%100000: progress(cnt,nlines,length=50)
progress(nlines,nlines,length=50)
print('finished '+str(nlines)+' records.')

print('analyzing stream... ')
nlines = sum(1 for line in open(cwd+fstream,'r'))-1
if not nlines: sys.exit()
with open(cwd+fstream,'r') as spay:
    # open output files
    out = [open(cwd+fout[x],'w') for x in range(3)]
    next(spay)          # ignore header line
    for cnt,line in enumerate(spay):
        transac = ['unverified','unverified','unverified']
        data = line.split(',')
        if ''.join(data[0][x] for x in [4,7,13,16])=='--::':
            if len(data)>3:
                id1, id2 = data[1].strip(), data[2].strip()
                gpath = bisearch(Gneib,id1,id2)
                if gpath < 2: transac[0] = 'trusted'
                if gpath < 3: transac[1] = 'trusted'
                if gpath < 5: transac[2] = 'trusted'
                if gpath > 1: neibadd(id1,id2)       # update neighbor list
            [out[x].write(transac[x]+'\n') for x in range(3)]
        if not cnt%100000: progress(cnt,nlines,length=50)
    [out[x].close() for x in range(3)]
progress(nlines,nlines,length=50)
print('finished '+str(nlines)+' records.')
