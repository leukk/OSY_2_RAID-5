#ifndef __PROGTEST__
#include <cstdio>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cassert>
#include <stdexcept>
using namespace std;


// Size of one sector in bytes, the basic unit for all operations
constexpr int SECTOR_SIZE = 8;
// Maximum number of devices in RAID
constexpr int MAX_RAID_DEVICES = 16;
// Minimum number of devices in RAID
constexpr int MIN_RAID_DEVICES = 3;
// Maximum number of sectors on one disk
constexpr int MAX_DEVICE_SECTORS = 1024 * 1024 * 2;
// Minimum number of sectors on one disk
// constexpr int MIN_DEVICE_SECTORS = 1 * 1024 * 2;
constexpr int MIN_DEVICE_SECTORS = 6;

constexpr int RAID_STOPPED = 0; // Not assembled (before calling start)
constexpr int RAID_OK = 1; // Operating correctly
constexpr int RAID_DEGRADED = 2; // One disk has failed
constexpr int RAID_FAILED = 3; // Not operating correctly, 2+ disks have failed

/// Struct representing a drive device driver
/// Used by CRaidVolume for basic operations
/// m_Read/m_Write interface:
/// arg1: Drive index (0... m_Devices-1)
/// arg2: Index of first sector to read/write from (0... m_Sectors-1)
/// arg3: Ptr to memory to read/write from
/// arg4: Number of sectors to read/write
/// \return Number of sectors successfully written/read from
struct TBlkDev {
    int m_Devices; // Number of used drives
    int m_Sectors; // Number of sectors per drive

    int (*m_Read)(int, int, void *, int); // Read function ptr

    int (*m_Write)(int, int, const void *, int); // Write function ptr
};

#endif /* __PROGTEST__ */

struct CDriveMetadata {
    CDriveMetadata() = default;

    explicit CDriveMetadata(const int failed_drive_index, const int timestamp)
        : m_failed_drive_i(failed_drive_index), m_timestamp(timestamp) {
    };

    int m_failed_drive_i = -1;
    int m_timestamp = 1;
};

constexpr int FAILED_DRIVE_INDEX = 0;
constexpr int TIMESTAMP_INDEX = 1;

// Stack allocated buffer macros
#define INT_SECTOR_BUFFER(NAME) int NAME[SECTOR_SIZE/sizeof(int)] = {}
#define CHAR_SECTOR_BUFFER(NAME) char NAME[SECTOR_SIZE] = {}

class CRaidVolume {
public:
    CRaidVolume();

    ~CRaidVolume();

    /// Initializes TBlkDev drives with metadata
    /// @param dev TBlkDev interface
    /// @return False if failed, true if succeeded
    static bool create(const TBlkDev &dev);

    /// 
    /// @param dev 
    /// @return 
    int start(const TBlkDev &dev);

    /// 
    /// @return 
    int stop();

    /// Resynchronizes drives in case of RAID_DEGRADED
    /// @return int, RAID status
    int resync();

    /// Returns current RAID status
    /// @return int, RAID status
    int status() const;

    /// Returns size of usable raid sectors
    /// \return int, number of usable sectors
    int size() const;

    /// Reads secCnt sectors to data starting from a given raid sector index (secNr)
    /// \param secNr starting raid sector index
    /// \param data memory to write data to
    /// \param secCnt number of sectors to read
    /// \return bool, operation success
    bool read(int secNr, void *data, int secCnt);

    /// Writes secCnt sectors from data starting from a given raid sector index (secNr)
    /// \param secNr starting raid sector index
    /// \param data memory to read (write) data from
    /// \param secCnt number of sectors to write
    /// \return bool, operation success
    bool write(int secNr, const void *data, int secCnt);

protected:
    /// Checks tblkdev validity
    /// @param dev tblkdev instance
    /// @return bool, validity of dev
    static bool checkTBlkDev(const TBlkDev &dev);

    /// Calculate physical drive, sector and parity drive indices based on a raid sector index
    /// @param raid_sector input raid sector index
    /// @param drive_i output drive index
    /// @param drive_sector_i output drive sector index
    /// @param parity_drive_i output parity drive index
    void raidSectorToPhysical(int raid_sector, int &drive_i, int &drive_sector_i, int &parity_drive_i) const;

    /// Clears & resets all member variables to default
    /// (frees m_dev ptr)
    void clearRaidVolumeData();

    // Tblkdev interface ptr
    TBlkDev *m_dev = nullptr;
    // Metadata sector index & metadata ptr
    int m_metadata_sector = 0;
    CDriveMetadata m_metadata = {};
    // Current RAID status
    int m_status = RAID_STOPPED;
    // Current RAID size
    int m_raid_size = 0;
    // Member R/W buffer
    INT_SECTOR_BUFFER(m_buffer);
};

CRaidVolume::CRaidVolume() {
    clearRaidVolumeData();
}

CRaidVolume::~CRaidVolume() {
    clearRaidVolumeData();
}

bool CRaidVolume::create(const TBlkDev &dev) {
    if (!checkTBlkDev(dev))
        return false;

    // Check if sector_size is too small for metadata
    if constexpr (SECTOR_SIZE < sizeof(CDriveMetadata))
        return false;

    // Create stack int buffer
    INT_SECTOR_BUFFER(buffer);
    memset(&buffer, 0, SECTOR_SIZE);

    buffer[TIMESTAMP_INDEX] = 0;
    buffer[FAILED_DRIVE_INDEX] = -1;

    // Try write default metadata to all drives
    const int metadata_sector_i = dev.m_Sectors - 1;
    for (int dev_i = 0; dev_i < dev.m_Devices; dev_i++) {
        if (dev.m_Write(dev_i, metadata_sector_i, &buffer, 1) == 1)
            continue;

        // Initializing more than 2 drives failed
        if(buffer[FAILED_DRIVE_INDEX] != -1)
            return false;

        // Initializing one drive failed, RAID DEGRADED, rewrite starting info
        buffer[FAILED_DRIVE_INDEX] = dev_i;
        dev_i = 0;
    }

    return true;
}

int CRaidVolume::start(const TBlkDev &dev) {
    // RAID volume was not stopped before calling start
    if (m_dev != nullptr || !checkTBlkDev(dev))
        return RAID_FAILED;

    // Initialize RAID, Copy TBlkDev
    m_dev = new TBlkDev;
    m_dev->m_Devices = dev.m_Devices;
    m_dev->m_Sectors = dev.m_Sectors;
    m_dev->m_Read = dev.m_Read;
    m_dev->m_Write = dev.m_Write;

    // Assume RAID_OK before checking metadata
    m_status = RAID_OK;

    // Calculate raid size
    const int usable_sector_count = (m_dev->m_Devices * m_dev->m_Sectors) - m_dev->m_Devices;
    m_raid_size = usable_sector_count - (usable_sector_count / dev.m_Devices);

    // Initialize metadata sector & m_drives_metadata array
    m_metadata_sector = m_dev->m_Sectors - 1;

    // Only read metadata from the first drive for now
    memset(&m_buffer, 0, SECTOR_SIZE);
    if (m_dev->m_Read(0, m_metadata_sector, &m_buffer, 1) != 1)
        return RAID_FAILED;

    m_metadata.m_failed_drive_i = -1;
    m_metadata.m_timestamp = m_buffer[1];

    // // Read metadata of every drive to metadata array
    // for(int dev_i = 0; dev_i < m_dev->m_Devices; dev_i++) {
    //     if (m_dev->m_Read(dev_i, m_metadata_sector, &read_buffer, 1) != 1) {
    //         m_drives_metadata[dev_i].m_failed_drive_i = 0;
    //         m_drives_metadata[dev_i].m_timestamp = 0;
    //     } else {
    //         m_drives_metadata[dev_i].m_failed_drive_i = read_buffer[0];
    //         m_drives_metadata[dev_i].m_timestamp = read_buffer[1];
    //     }
    //
    //     // Skip RAID status initialization until 3rd drive
    //     if (dev_i < 2)
    //         continue;
    //
    //     auto current = m_drives_metadata[dev_i];
    //     auto one_prev = m_drives_metadata[dev_i-1];
    //     auto two_prev = m_drives_metadata[dev_i-2];
    //
    //     if(current.m_timestamp == one_prev.m_timestamp && one_prev.m_timestamp == two_prev.m_timestamp)
    //         continue;
    // }
    //
    // delete [] m_drives_metadata;

    return m_status;
}

int CRaidVolume::stop() {
    if(m_status == RAID_STOPPED || m_status == RAID_FAILED)
        return m_status = RAID_STOPPED;

    // Increment current metadata timestamp
    m_metadata.m_timestamp += 1;

    // Load metadata to m_buffer
    memset(&m_buffer, 0, SECTOR_SIZE);
    m_buffer[FAILED_DRIVE_INDEX] = m_metadata.m_failed_drive_i;
    m_buffer[TIMESTAMP_INDEX] = m_metadata.m_timestamp;

    // Write metadata information to all drives
    for (int dev_i = 0; dev_i < m_dev->m_Devices; dev_i++) {
        if(m_dev->m_Write(dev_i, m_metadata_sector, &m_buffer, 1) == 1)
            continue;

        // Skip trying to correct writing to a degraded drive or a failed RAID
        if(m_metadata.m_failed_drive_i == dev_i)
            continue;

        // Raid degraded while stopping, rewrite buffer info
        if(m_status == RAID_OK) {
            m_metadata.m_failed_drive_i = dev_i;
            m_status = RAID_DEGRADED;
            m_buffer[FAILED_DRIVE_INDEX] = m_metadata.m_failed_drive_i;
            dev_i = 0;
            continue;
        }

        // RAID failed while stopping, rewrite metadata without chekcing
        if(m_status == RAID_DEGRADED) {
            m_status = RAID_FAILED;
            dev_i = 0;
        }
    }

    // Clear CRaidVolume data
    clearRaidVolumeData();

    return m_status;
}

int CRaidVolume::resync() {
    return m_status;
}

int CRaidVolume::status() const {
    return m_status;
}

int CRaidVolume::size() const {
    return m_raid_size;
}

bool CRaidVolume::read(int secNr, void *data, int secCnt) {
    // Read buffer nullptr or Invalid starting raid sector
    if (!data || secCnt < 0 || secCnt > (m_raid_size - 1) || m_status == RAID_FAILED)
        return false;

    for (int raid_i = secNr; raid_i < (secNr + secCnt); raid_i++) {
        // Reading past existing raid sectors
        if (raid_i >= m_raid_size)
            return false;

        // Translate raid index to "physical" drive/sector/parity_drive indices
        int drive_i = 0;
        int drive_sector_i = 0;
        int parity_drive_i = 0;
        raidSectorToPhysical(raid_i, drive_i, drive_sector_i, parity_drive_i);

        // Try read data from "FAIL" drive - use parity
        // TODO: Use parity instead of faulty drive
        if(m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i == drive_i) {
            if (m_dev->m_Read(drive_i, drive_sector_i, data, 1) != 1) {
                // Reading parity failed, 2+ drives failed, raid failed
                m_status = RAID_FAILED;
                return false;
            }
        }

        // Try read data from "OK" drive in degraded state
        if(m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i != drive_i) {
            if (m_dev->m_Read(drive_i, drive_sector_i, data, 1) != 1) {
                // Current drive failed in addition to other degraded drive
                m_status = RAID_FAILED;
                return false;
            }
        }

        // RAID must be RAID_OK, read
        if (m_dev->m_Read(drive_i, drive_sector_i, data, 1) != 1) {
            // Current drive failed, set raid to degraded state
            m_status = RAID_DEGRADED;
            m_metadata.m_failed_drive_i = drive_i;

            // Repeat read in degraded state
            raid_i--;
            continue;
        }

        // Increment buffer pointer
        data += SECTOR_SIZE;
    }

    return true;
}

bool CRaidVolume::write(int secNr, const void *data, int secCnt) {
    // Write buffer nullptr or Invalid starting raid sector
    if (!data || secCnt < 0 || secCnt > (m_raid_size - 1) || m_status == RAID_FAILED)
        return false;

    for (int raid_i = secNr; raid_i < (secNr + secCnt); raid_i++) {
        // Writing past existing raid sectors
        if (raid_i >= m_raid_size)
            return false;

        // Translate raid index to "physical" drive/sector/parity_drive indices
        int drive_i = 0;
        int drive_sector_i = 0;
        int parity_drive_i = 0;
        raidSectorToPhysical(raid_i, drive_i, drive_sector_i, parity_drive_i);

        // Try write data from "FAIL" drive - use parity
        // TODO: Use parity instead of faulty drive
        if(m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i == drive_i) {
            if (m_dev->m_Write(drive_i, drive_sector_i, data, 1) != 1) {
                // Writing parity failed, 2+ drives failed, raid failed
                m_status = RAID_FAILED;
                return false;
            }
        }

        // Try read data from "OK" drive in degraded state
        if(m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i != drive_i) {
            if (m_dev->m_Write(drive_i, drive_sector_i, data, 1) != 1) {
                // Current drive failed in addition to other degraded drive
                m_status = RAID_FAILED;
                return false;
            }
        }

        // RAID must be RAID_OK, write
        if (m_dev->m_Write(drive_i, drive_sector_i, data, 1) != 1) {
            // Current drive failed, set raid to degraded state
            m_status = RAID_DEGRADED;
            m_metadata.m_failed_drive_i = drive_i;

            // Repeat write in degraded state
            raid_i--;
            continue;
        }

        // Increment buffer pointer
        data += SECTOR_SIZE;
    }

    return true;
}

bool CRaidVolume::checkTBlkDev(const TBlkDev &dev) {
    if (dev.m_Devices < 3 || dev.m_Devices > MAX_RAID_DEVICES)
        return false;
    if (dev.m_Sectors < MIN_DEVICE_SECTORS || dev.m_Sectors > MAX_DEVICE_SECTORS)
        return false;
    if (!dev.m_Read)
        return false;
    if (!dev.m_Write)
        return false;
    return true;
}

void CRaidVolume::raidSectorToPhysical(const int raid_sector, int &drive_i, int &drive_sector_i,
                                       int &parity_drive_i) const {
    // const int mod = m_dev->m_Devices;
    //
    // const int skipped_parities = 1 + (int) (raid_sector / mod);
    // const int phys_sector = raid_sector + skipped_parities;
    //
    // drive_i = phys_sector % mod;
    // drive_sector_i = phys_sector / mod;
    // parity_drive_i = (phys_sector / mod) % mod;

    drive_i = raid_sector / m_dev->m_Sectors;
    drive_sector_i = raid_sector % m_dev->m_Sectors;
    parity_drive_i = m_dev->m_Devices - 1;
}

void CRaidVolume::clearRaidVolumeData() {
    // Free & reset heap variables
    delete m_dev;
    m_dev = nullptr;

    // Reset stack variables
    m_metadata_sector = 0;
    m_metadata = {};
    m_status = RAID_STOPPED;
    m_raid_size = 0;
}

#ifndef __PROGTEST__

#include "custom.inc"

#endif /* __PROGTEST__ */
