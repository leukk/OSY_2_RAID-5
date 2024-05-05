/* SW RAID6 - basic test
 *
 * The testing of the RAID driver requires a backend (simulating the underlying
 * disks). Next, the tests of your RAID implemetnation are needed. To help you
 * with the implementation, a sample backend is implemented in this file. It
 * provides a quick-and-dirty implementation of the underlying disks (simulated
 * in files) and a few Raid... function calls.
 *
 * The implementation in the real testing environment is different. The sample
 * below is a minimalistic disk backend which matches the required interface.
 * The backend, for instance, cannot simulate a crashed disk. To test your Raid
 * implementation, you will have to modify or extend the backend.
 *
 * Next, you will have to add some raid testing. There is a few Raid...
 * functions called from within main(), however, the tests are incomplete. For
 * instance, RaidResync () is not tested here. Once again, this is only a
 * starting point.
 */
#include <cstdio>
#include <ctime>
#include <random>
#include <unistd.h>
#include <memory>
#include <cassert>
#include <cstring>

#define println(...)
#define dbg(arg) printf("%s\n", arg);
#define info(...)
#define warn(...)
#define error(...)

constexpr int RAID_DEVICES = 4;
constexpr int DISK_SECTORS = 8192 * 2;
static FILE * g_Fp[RAID_DEVICES];

//-------------------------------------------------------------------------------------------------
/** Sample sector reading function. The function will be called by your Raid
 * driver implementation. Notice, the function is not called directly. Instead,
 * the function will be invoked indirectly through function pointer in the
 * TBlkDev structure.
 */
int diskRead(int device, int sectorNr, void * data, int sectorCnt) {
    if (device < 0 || device >= RAID_DEVICES)
        return 0;
    if (g_Fp[device] == NULL)
        return 0;
    if (sectorCnt <= 0 || sectorNr + sectorCnt > DISK_SECTORS)
        return 0;
    fseek(g_Fp[device], sectorNr * SECTOR_SIZE, SEEK_SET);
    return fread(data, SECTOR_SIZE, sectorCnt, g_Fp[device]);
}

int ONE_FAIL = 0;
int TWO_FAIL[2] = {0, 1};

int diskReadBadOne(int device, int sectorNr, void * data, int sectorCnt) {
    if (device == ONE_FAIL) {
        return -1;
    }
    return diskRead(device, sectorNr, data, sectorCnt);
}

int diskReadBadTwo(int device, int sectorNr, void * data, int sectorCnt) {
    if (device == TWO_FAIL[0] || device == TWO_FAIL[1]) {
        return -1;
    }
    return diskRead(device, sectorNr, data, sectorCnt);
}

int diskReadBadAll(int, int, void *, int) { return -1; }

int diskReadBadAllRand(int device, int sectorNr, void * data, int sectorCnt) {
    if (rand() < (RAND_MAX / 2)) {
        return diskReadBadAll(device, sectorNr, data, sectorCnt);
    }
    return diskRead(device, sectorNr, data, sectorCnt);
}

//-------------------------------------------------------------------------------------------------
/** Sample sector writing function. Similar to diskRead
 */
int diskWrite(int device, int sectorNr, const void * data, int sectorCnt) {
    if (device < 0 || device >= RAID_DEVICES)
        return 0;
    if (g_Fp[device] == NULL)
        return 0;
    if (sectorCnt <= 0 || sectorNr + sectorCnt > DISK_SECTORS)
        return 0;
    fseek(g_Fp[device], sectorNr * SECTOR_SIZE, SEEK_SET);
    return fwrite(data, SECTOR_SIZE, sectorCnt, g_Fp[device]);
}

int diskWriteBadOne(int device, int sectorNr, const void * data,
                    int sectorCnt) {
    if (device == ONE_FAIL) {
        return -1;
    }
    return diskWrite(device, sectorNr, data, sectorCnt);
}

int diskWriteBadTwo(int device, int sectorNr, const void * data,
                    int sectorCnt) {
    if (device == TWO_FAIL[0] || device == TWO_FAIL[1]) {
        return -1;
    }
    return diskWrite(device, sectorNr, data, sectorCnt);
}

int diskWriteBadAll(int, int, const void *, int) { return -1; }

int diskWriteBadAllRand(int device, int sectorNr, const void * data,
                        int sectorCnt) {
    if (rand() < (RAND_MAX / 2)) {
        return diskWriteBadAll(device, sectorNr, data, sectorCnt);
    }
    return diskWrite(device, sectorNr, data, sectorCnt);
}

//-------------------------------------------------------------------------------------------------
/** A function which releases resources allocated by openDisks/createDisks
 */
void doneDisks() {
    for (int i = 0; i < RAID_DEVICES; i++)
        if (g_Fp[i]) {
            fclose(g_Fp[i]);
            g_Fp[i] = NULL;
        }
}
//-------------------------------------------------------------------------------------------------
/** A function which creates the files needed for the sector reading/writing
 * functions above. This function is only needed for the particular
 * implementation above.
 */
TBlkDev createDisks() {
    char buffer[SECTOR_SIZE];
    TBlkDev res;
    char fn[100];

    memset(buffer, 0, sizeof(buffer));

    for (int i = 0; i < RAID_DEVICES; i++) {
        snprintf(fn, sizeof(fn), "/tmp/%04d", i);
        g_Fp[i] = fopen(fn, "w+b");
        if (!g_Fp[i]) {
            doneDisks();
            throw "Raw storage create error";
        }

        for (int j = 0; j < DISK_SECTORS; j++)
            if (fwrite(buffer, sizeof(buffer), 1, g_Fp[i]) != 1) {
                doneDisks();
                throw "Raw storage create error";
            }
    }

    res.m_Devices = RAID_DEVICES;
    res.m_Sectors = DISK_SECTORS;
    res.m_Read = diskRead;
    res.m_Write = diskWrite;
    return res;
}
//-------------------------------------------------------------------------------------------------
/** A function which opens the files needed for the sector reading/writing
 * functions above. This function is only needed for the particular
 * implementation above.
 */
TBlkDev openDisks() {
    TBlkDev res;
    char fn[100];

    for (int i = 0; i < RAID_DEVICES; i++) {
        snprintf(fn, sizeof(fn), "/tmp/%04d", i);
        g_Fp[i] = fopen(fn, "r+b");
        if (!g_Fp[i]) {
            doneDisks();
            throw "Raw storage access error";
        }
        fseek(g_Fp[i], 0, SEEK_END);
        if (ftell(g_Fp[i]) != DISK_SECTORS * SECTOR_SIZE) {
            doneDisks();
            throw "Raw storage read error";
        }
    }
    res.m_Devices = RAID_DEVICES;
    res.m_Sectors = DISK_SECTORS;
    res.m_Read = diskRead;
    res.m_Write = diskWrite;
    return res;
}
//-------------------------------------------------------------------------------------------------

void degraded_test() {
    dbg("negr degraded test!");

    auto dev = createDisks();
    assert(CRaidVolume::create(dev));

    dev.m_Read = diskReadBadOne;
    dev.m_Write = diskWriteBadOne;

    CRaidVolume vol;
    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    constexpr int SEC_LIMIT = 10;

    char buffer[SECTOR_SIZE * SEC_LIMIT] = {};
    char check[SECTOR_SIZE * SEC_LIMIT] = {};

    std::random_device r;
    for (int i = 0; i < SEC_LIMIT; i++) {
        memset(buffer + i * SECTOR_SIZE, r() % 127, SECTOR_SIZE);
        assert(vol.write(0, buffer, SEC_LIMIT));
        assert(vol.read(0, check, SEC_LIMIT));
        assert(!memcmp(buffer, check, SECTOR_SIZE * SEC_LIMIT));
    }

    dbg("negr degraded - OK!");
}

void negr() {
    dbg("negr test!");

    TBlkDev dev = createDisks();
    assert(CRaidVolume::create(dev));
    CRaidVolume vol;
    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    std::random_device r;
    std::unique_ptr<char[][SECTOR_SIZE]> ptr = std::make_unique<char[][SECTOR_SIZE]>(vol.size());
    for (int i = 0; i < vol.size(); i++) {
        memset(ptr[i], r() % 127, SECTOR_SIZE);
    }
    constexpr int SC_NUMBER = 10;
    for (int i = 0; i < vol.size(); i++) {
        if (i + SC_NUMBER >= vol.size()) {
            break;
        }
        std::unique_ptr<char[]> check = std::make_unique<char[]>(SECTOR_SIZE * SC_NUMBER);
        char * walker = check.get();
        for (int j = i; j < i + SC_NUMBER; j++) {
            memcpy(walker, ptr[j], SECTOR_SIZE);
            walker += SECTOR_SIZE;
        }
        assert(vol.write(i, check.get(), SC_NUMBER));
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(SECTOR_SIZE * SC_NUMBER);
        assert(vol.read(i, buf.get(), SC_NUMBER));
        assert(!memcmp(buf.get(), check.get(), SECTOR_SIZE * SC_NUMBER));
    }
    dbg("negr test - OK!");
}

void resync_test() {
    dbg("borec resync test!");

    TBlkDev dev = createDisks();
    assert(CRaidVolume::create(dev));
    CRaidVolume vol;
    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    std::random_device r;
    constexpr int SEC_COUNT = 100;
    constexpr int BUFFER_ALLOC = SECTOR_SIZE * SEC_COUNT;
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(BUFFER_ALLOC);
    std::unique_ptr<char[]> check = std::make_unique<char[]>(BUFFER_ALLOC);

    for (int i = 0; i < BUFFER_ALLOC; i++) {
        memset(buffer.get() + i, r() % 127, 1);
    }

    assert(vol.write(0, buffer.get(), SEC_COUNT));
    assert(vol.read(0, check.get(), SEC_COUNT));
    assert(!memcmp(buffer.get(), check.get(), BUFFER_ALLOC));

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    dev.m_Read = diskReadBadOne;
    dev.m_Write = diskWriteBadOne;

    assert(vol.start(dev) == RAID_DEGRADED);

    assert(vol.read(0, check.get(), SEC_COUNT));
    assert(!memcmp(buffer.get(), check.get(), BUFFER_ALLOC));

    assert(vol.status() == RAID_DEGRADED);
    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    dev.m_Read = diskRead;
    dev.m_Write = diskWrite;

    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);
    assert(vol.resync() == RAID_OK);

    assert(vol.read(0, check.get(), SEC_COUNT));
    assert(!memcmp(buffer.get(), check.get(), BUFFER_ALLOC));
    dbg("borec resync test - OK!");
}

inline void fillSectorWithRandom(char sector[SECTOR_SIZE]) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255); // Distribution for random byte values

    for (auto byte_i = 0; byte_i < SECTOR_SIZE; byte_i++) {
        sector[byte_i] = static_cast<char>(dis(gen));
    }
}

void test0() {
    CRaidVolume vol;

    dbg("Test ukladania dat");

    TBlkDev dev = createDisks();
    assert(CRaidVolume::create(dev));
    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255); // Distribution for random byte values

    auto check = new char[vol.size()][SECTOR_SIZE];

    for (auto raid_i = 0; raid_i < vol.size(); raid_i++) {
        fillSectorWithRandom(check[raid_i]);
        assert(vol.write(raid_i, check[raid_i], 1));
    }

    for (auto raid_i = 0; raid_i < vol.size(); raid_i++) {
        char read_buffer[SECTOR_SIZE];

        assert(vol.read(raid_i, read_buffer, 1));

        for (int byte_i = 0; byte_i < SECTOR_SIZE; byte_i++) {
            if(read_buffer[byte_i] != check[raid_i][byte_i])
                assert("Data mismatch fail");
        }
    }

    dbg("Passed the test");
}

void test1() {
    CRaidVolume vol;

    dbg("default test!");

    TBlkDev dev = createDisks();
    assert(CRaidVolume::create(dev));
    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];

        assert(vol.read(i, buffer, 1));
        assert(vol.write(i, buffer, 1));
    }

    char buf[SECTOR_SIZE];
    memset(buf, 69, SECTOR_SIZE);

    for (int i = 0; i < vol.size(); i++) {
        memset(buf, i % 127, sizeof(buf));
        assert(vol.write(i, buf, 1));

        char buffer[SECTOR_SIZE];
        assert(vol.read(i, buffer, 1));
        assert(!memcmp(buf, buffer, SECTOR_SIZE));
    }

    assert(vol.status() == RAID_OK);
    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();

    negr();
    degraded_test();
    resync_test();

    dbg("testing bad write disk (all)!");

    dev = createDisks();
    dev.m_Write = diskWriteBadAll;

    assert(!CRaidVolume::create(dev));
    doneDisks();

    dbg("testing bad write disk (one)!");

    dev = createDisks();
    ONE_FAIL = 0;
    dev.m_Write = diskWriteBadOne;

    assert(!CRaidVolume::create(dev));
    doneDisks();

    dbg("testing bad read disk (all)!");

    dev = createDisks();
    dev.m_Read = diskReadBadAll;

    assert(CRaidVolume::create(dev));
    assert(vol.start(dev) == RAID_FAILED);
    assert(vol.status() == RAID_FAILED);
    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);
    doneDisks();

    dbg("random disk test!");

    dev = createDisks();
    dev.m_Read = diskReadBadAllRand;
    dev.m_Write = diskWriteBadAllRand;

    CRaidVolume::create(dev);
    vol.start(dev);
    if (vol.status() == RAID_OK || vol.status() == RAID_DEGRADED) {
        info("random disk array started! condition: %s",
             (vol.status() == RAID_OK) ? "OK" : "DEGRADED");
        for (int i = 0; i < vol.size(); ++i) {
            char buf[SECTOR_SIZE];
            vol.read(i, buf, 1);
            vol.write(i, buf, 1);
        }
    }
    vol.stop();
    doneDisks();

    dbg("some test!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        assert(vol.read(i, buffer, 1));
        for (size_t j = 0; j < sizeof(buffer); ++j) {
            if (i % 127 != buffer[j]) {
                error("%d != %d", i % 127, buffer[j]);
                assert(i % 127 == buffer[j]);
            }
        }
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();

    dbg("some test 2!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        assert(vol.read(i, buffer, 1));
        for (size_t j = 0; j < sizeof(buffer); ++j) {
            assert((i % 127) == buffer[j]);
        }
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();

    dbg("one disk read fail after write test!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));

        if(i == 16383) {
            dbg("This is where shit hits the fan");
        }

        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    // dev.m_Read = diskReadBadOne;

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        if(i == 16383) {
            dbg("This is where shit hits the fan");
        }
        char buffer[SECTOR_SIZE];
        assert(vol.read(i, buffer, 1));
        for (size_t j = 0; j < sizeof(buffer); ++j) {
            if (i % 127 != buffer[j]) {
                error("%d != %d", i % 127, buffer[j]);
                assert(i % 127 == buffer[j]);
            }
        }
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();

    dbg("two disk read fail after write test!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    dev.m_Read = diskReadBadTwo;

    assert(vol.start(dev) == RAID_FAILED);
    assert(vol.status() == RAID_FAILED);

    doneDisks();

    dbg("resync fail test!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    ONE_FAIL = 0;
    dev.m_Read = diskReadBadOne;
    dev.m_Write = diskWriteBadOne;

    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, (vol.size() - i) % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_DEGRADED);

    assert(vol.resync() == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    ONE_FAIL = 1; // other disk will fail
    assert(vol.resync() == RAID_FAILED);
    assert(vol.status() == RAID_FAILED);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();

    dbg("resync success test!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    ONE_FAIL = 0;
    dev.m_Read = diskReadBadOne;
    dev.m_Write = diskWriteBadOne;

    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, (vol.size() - i) % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_DEGRADED);

    assert(vol.resync() == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    ONE_FAIL = -1; // no disk will fail
    assert(vol.resync() == RAID_OK);
    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();

    dbg("bad disk during stop!");

    ONE_FAIL = -1; // no disk bad

    dev = createDisks();
    dev.m_Read = diskReadBadOne;
    dev.m_Write = diskWriteBadOne;
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_OK);

    ONE_FAIL = 0;
    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    doneDisks();

    dbg("degrade, stop, start, resync!");

    dev = createDisks();
    assert(CRaidVolume::create(dev));

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_OK);
    assert(vol.status() == RAID_OK);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, i % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    ONE_FAIL = 0;
    dev.m_Read = diskReadBadOne;
    dev.m_Write = diskWriteBadOne;

    vol = CRaidVolume();

    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    for (int i = 0; i < vol.size(); i++) {
        char buffer[SECTOR_SIZE];
        memset(buffer, (vol.size() - i) % 127, sizeof(buffer));
        assert(vol.write(i, buffer, 1));
    }

    assert(vol.status() == RAID_DEGRADED);

    assert(vol.resync() == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    vol = CRaidVolume();

    ONE_FAIL = -1; // no disk will fail
    assert(vol.start(dev) == RAID_DEGRADED);
    assert(vol.status() == RAID_DEGRADED);

    assert(vol.resync() == RAID_OK);
    assert(vol.status() == RAID_OK);

    assert(vol.stop() == RAID_STOPPED);
    assert(vol.status() == RAID_STOPPED);

    doneDisks();
}

int main() {
    srand(time(nullptr));
    test0();
    test1();
    return EXIT_SUCCESS;
}
