### Dstat most expensive I/O process plugin
### Displays the name of the most expensive I/O process
###
### Authority: dag@wieers.com

class dstat_plugin(dstat):
    def __init__(self):
        self.name = 'most expensive'
        self.vars = ('i/o process',)
        self.type = 's'
        self.width = 22
        self.scale = 0
        self.pidset1 = {}; self.pidset2 = {}

    def check(self):
        if not os.access('/proc/self/io', os.R_OK):
            raise Exception, 'Kernel has no I/O accounting, use at least 2.6.20'

    def extract(self):
        self.val['usage'] = 0.0
        self.val['i/o process'] = ''
        for pid in proc_pidlist():
            try:
                ### Reset values
                if not self.pidset2.has_key(pid):
                    self.pidset2[pid] = {'rchar:': 0, 'wchar:': 0}
                if not self.pidset1.has_key(pid):
                    self.pidset1[pid] = {'rchar:': 0, 'wchar:': 0}

                ### Extract name
                name = proc_splitline('/proc/%s/stat' % pid)[1][1:-1]

                ### Extract counters
                for l in proc_splitlines('/proc/%s/io' % pid):
                    if len(l) != 2: continue
                    self.pidset2[pid][l[0]] = int(l[1])
            except IOError:
                continue
            except IndexError:
                continue

            read_usage = (self.pidset2[pid]['rchar:'] - self.pidset1[pid]['rchar:']) * 1.0 / elapsed
            write_usage = (self.pidset2[pid]['wchar:'] - self.pidset1[pid]['wchar:']) * 1.0 / elapsed
            usage = read_usage + write_usage
#            if usage > 0.0:
#                print '%s %s:%s' % (pid, read_usage, write_usage)

            ### Get the process that spends the most jiffies
            if usage > self.val['usage']:
                self.val['usage'] = usage
                self.val['read_usage'] = read_usage
                self.val['write_usage'] = write_usage
                self.val['pid'] = pid
                self.val['name'] = getnamebypid(pid, name)

        if step == op.delay:
            for pid in self.pidset2.keys():
                self.pidset1[pid].update(self.pidset2[pid])

        if self.val['usage'] != 0.0:
            self.val['i/o process'] = '%-*s%s %s' % (self.width-11, self.val['name'][0:self.width-11], cprint(self.val['read_usage'], 'd', 5, 1024), cprint(self.val['write_usage'], 'd', 5, 1024))

        ### Debug (show PID)
#        self.val['i/o process'] = '%*s %-*s%s %s' % (5, self.val['pid'], self.width-17, self.val['name'][0:self.width-17], cprint(self.val['read_usage'], 'd', 5, 1024), cprint(self.val['write_usage'], 'd', 5, 1024))

    def showcsv(self):
        return '%s / %d:%d' % (self.val['name'], self.val['read_usage'], self.val['write_usage'])

# vim:ts=4:sw=4:et
