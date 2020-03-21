from DeArchive import dearchive

arch = ["2020-03-19-ArchivDB", "2020-03-21-ArchivDB"]

i = 1

while i <= 500:
    dearchive("sbmd1db2", arch, 20)
    i += 1