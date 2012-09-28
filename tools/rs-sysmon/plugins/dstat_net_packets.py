### Author: Dag Wieers <dag@wieers.com>

class dstat_plugin(dstat):
    """
    Number of packets received and send per interface.
    """

    def __init__(self):
        self.nick = ('#recv', '#send')
        self.type = 'f'
        self.width = 5
        self.scale = 1000
        self.totalfilter = re.compile('^(lo|bond[0-9]+|face|.+\.[0-9]+)$')
        self.open('/proc/net/dev')
        self.cols = 2

    def discover(self, *objlist):
        ret = []
        for l in self.splitlines(replace=':'):
            if len(l) < 17: continue
            if l[2] == '0' and l[10] == '0': continue
            name = l[0]
            if name not in ('lo', 'face'):
                ret.append(name)
        ret.sort()
        for item in objlist: ret.append(item)
        return ret

    def vars(self):
        ret = []
        if op.netlist:
            varlist = op.netlist
        elif not op.full:
            varlist = ('total',)
        else:
            varlist = self.discover
#           if len(varlist) > 2: varlist = varlist[0:2]
            varlist.sort()
        for name in varlist:
            if name in self.discover + ['total', 'lo']:
                ret.append(name)
        if not ret:
            raise Exception, "No suitable network interfaces found to monitor"
        return ret

    def name(self):
        return ['pkt/'+name for name in self.vars]

    def extract(self):
        self.set2['total'] = [0, 0]
        for l in self.splitlines(replace=':'):
            if len(l) < 17: continue
            if l[2] == '0' and l[10] == '0': continue
            name = l[0]
            if name in self.vars :
                self.set2[name] = ( long(l[2]), long(l[10]) )
            if not self.totalfilter.match(name):
                self.set2['total'] = ( self.set2['total'][0] + long(l[2]), self.set2['total'][1] + long(l[10]))
        if update:
            for name in self.set2.keys():
                self.val[name] = (
                    (self.set2[name][0] - self.set1[name][0]) * 1.0 / elapsed,
                    (self.set2[name][1] - self.set1[name][1]) * 1.0 / elapsed,
                 )
        if step == op.delay:
            self.set1.update(self.set2)
