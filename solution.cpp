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
constexpr int MIN_DEVICE_SECTORS = 1 * 1024 * 2;

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
#define INT_SECTOR_BUFFER(NAME) int NAME[SECTOR_SIZE/sizeof(int)]
#define CHAR_SECTOR_BUFFER(NAME) char NAME[SECTOR_SIZE]

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
    static bool validate_t_blk_dev(const TBlkDev &dev);

    /// Calculate physical drive, sector and parity drive indices based on a raid sector index
    /// @param raid_sector input raid sector index
    /// @param drive_i output drive index
    /// @param drive_sector_i output drive sector index
    /// @param parity_drive_i output parity drive index
    void raid_sector_to_physical(int raid_sector, int &drive_i, int &drive_sector_i, int &parity_drive_i) const;

    /// Clears & resets all member variables to default
    /// (frees m_dev ptr)
    void clear_raid_volume_data();

    /// Returns data or parity by xoring sectors from other drives.
    /// @param out_buffer out, int buffer of SECTOR_SIZE Bytes
    /// @param dead_drive_i in, index of faulty drive
    /// @param sector_i in, index of faulty drive sector
    /// @return bool, reading success
    int xor_read_without_sector(INT_SECTOR_BUFFER(out_buffer), int dead_drive_i, int sector_i) const;

    int xor_get_parity_supplement_dead_sector(INT_SECTOR_BUFFER(out_buffer), int parity_drive_i,
                                              int dead_drive_i,
                                              const INT_SECTOR_BUFFER(dead_drive_supplement_buffer),
                                              int sector_i) const;

    /// Outputs xor of provided buffers into out_buffer
    /// @param out_buffer xor operand, output buffer
    /// @param in_buffer xor operand
    static inline void xor_int_buffers(INT_SECTOR_BUFFER(out_buffer), const INT_SECTOR_BUFFER(in_buffer));


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
    clear_raid_volume_data();
}

CRaidVolume::~CRaidVolume() {
    clear_raid_volume_data();
}

bool CRaidVolume::create(const TBlkDev &dev) {
    if (!validate_t_blk_dev(dev))
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
    for (int dev_i = 0; dev_i < dev.m_Devices; dev_i++)
        if (dev.m_Write(dev_i, metadata_sector_i, &buffer, 1) != 1)
            return false;

    return true;
}

int CRaidVolume::start(const TBlkDev &dev) {
    // RAID volume was not stopped before calling start
    if (m_dev != nullptr || !validate_t_blk_dev(dev))
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
    const int usable_sector_count = m_dev->m_Devices * (m_dev->m_Sectors - 1); // Number of non metadata sectors
    m_raid_size = usable_sector_count - (m_dev->m_Sectors - 1); // Subtract parity sectors - aka one for each line
    // Initialize metadata sector & m_drives_metadata array
    m_metadata_sector = m_dev->m_Sectors - 1;

    m_metadata.m_failed_drive_i = -1;
    m_metadata.m_timestamp = m_buffer[1];

    // Metadata read buffer
    INT_SECTOR_BUFFER(read_buffer);

    int timestamps[3];
    int failed_drives[3];
    int read_failed_cnt = 0;
    int read_failed_drive = -1;

    // Load metadata from first three drives
    for (int dev_i = 0; dev_i < 3; dev_i++) {
        if (m_dev->m_Read(dev_i, m_metadata_sector, &read_buffer, 1) != 1) {
            read_failed_drive = dev_i;
            read_failed_cnt++;
            continue;
        }
        timestamps[dev_i] = read_buffer[TIMESTAMP_INDEX];
        failed_drives[dev_i] = read_buffer[FAILED_DRIVE_INDEX];
    }

    // There was one read failure
    if (read_failed_cnt == 1) {
        auto other_a = (read_failed_drive + 1) % 3;
        auto other_b = (read_failed_drive + 2) % 3;

        // Timestamps of other two successfully read drives dont match
        if (timestamps[other_a] != timestamps[other_b]) {
            m_status = RAID_FAILED;
            return m_status;
        }

        // There was no known failed drive when shutting down
        if (failed_drives[other_a] < 0) {
            m_status = RAID_DEGRADED;
            m_metadata.m_failed_drive_i = read_failed_drive;
            m_metadata.m_timestamp = timestamps[other_a];
            return m_status;
        }

        // Timestamps match but drives know another failed drive
        m_status = RAID_FAILED;
        return m_status;
    }
    if (read_failed_cnt == 0) {
        if (timestamps[0] != timestamps[1] && timestamps[1] != timestamps[2] && timestamps[0] != timestamps[2]) {
            // All timestamps are different - raid must have at least 2 failed drives
            m_status = RAID_FAILED;
            return m_status;
        }
        if (timestamps[0] == timestamps[1] && timestamps[1] == timestamps[2]) {
            // All timestamps match, set metadata
            m_metadata.m_timestamp = timestamps[0];
            m_metadata.m_failed_drive_i = failed_drives[0];
            if (m_metadata.m_failed_drive_i == -1)
                m_status = RAID_OK;
            else
                m_status = RAID_DEGRADED;
        } else if (timestamps[0] == timestamps[1] && failed_drives[0] == 2) {
            // Third drive failed
            m_metadata.m_timestamp = timestamps[0];
            m_metadata.m_failed_drive_i = 2;
            m_status = RAID_DEGRADED;
        } else if (timestamps[0] == timestamps[2] && failed_drives[0] == 1) {
            // Second drive failed
            m_metadata.m_timestamp = timestamps[0];
            m_metadata.m_failed_drive_i = 1;
            m_status = RAID_DEGRADED;
        } else if (timestamps[1] == timestamps[2] && failed_drives[1] == 0) {
            // First drive failed
            m_metadata.m_timestamp = timestamps[1];
            m_metadata.m_failed_drive_i = 0;
            m_status = RAID_DEGRADED;
        } else {
            // Timestamp is different while OK drives say another drive is faulty
            m_status = RAID_FAILED;
            return m_status;
        }
    } else {
        m_status = RAID_FAILED;
        return m_status;
    }

    return m_status;
}

int CRaidVolume::stop() {
    if (m_status == RAID_STOPPED || m_status == RAID_FAILED)
        return m_status = RAID_STOPPED;

    // Increment current metadata timestamp
    m_metadata.m_timestamp += 1;

    // Load metadata to m_buffer
    memset(&m_buffer, 0, SECTOR_SIZE);
    m_buffer[FAILED_DRIVE_INDEX] = m_metadata.m_failed_drive_i;
    m_buffer[TIMESTAMP_INDEX] = m_metadata.m_timestamp;

    // Write metadata information to all drives
    for (int dev_i = 0; dev_i < m_dev->m_Devices; dev_i++) {
        if (m_dev->m_Write(dev_i, m_metadata_sector, &m_buffer, 1) == 1)
            continue;

        // Skip trying to correct writing to a degraded drive or a failed RAID
        if (m_metadata.m_failed_drive_i == dev_i)
            continue;

        // Raid degraded while stopping, rewrite buffer info
        if (m_status == RAID_OK) {
            m_metadata.m_failed_drive_i = dev_i;
            m_status = RAID_DEGRADED;
            m_buffer[FAILED_DRIVE_INDEX] = m_metadata.m_failed_drive_i;
            dev_i = 0;
            continue;
        }

        // RAID failed while stopping, rewrite metadata without checking
        if (m_status == RAID_DEGRADED) {
            m_status = RAID_FAILED;
            dev_i = 0;
        }
    }

    // Clear CRaidVolume data
    clear_raid_volume_data();

    return m_status;
}

int CRaidVolume::resync() {
    if (m_status == RAID_OK || m_status == RAID_FAILED || m_status == RAID_STOPPED)
        return m_status;

    INT_SECTOR_BUFFER(restore_buffer) = {};

    for (int sector_i = 0; sector_i < (m_dev->m_Sectors - 1); sector_i++) {

        // Get original drive data from parity
        if (xor_read_without_sector(restore_buffer, m_metadata.m_failed_drive_i, sector_i) >= 0) {
            // One of other drives failed while restoring data
            m_status = RAID_FAILED;
            return m_status;
        }

        // Try write data to the possibly OK degraded drive
        if (m_dev->m_Write(m_metadata.m_failed_drive_i, sector_i, restore_buffer, 1) != 1) {
            m_status = RAID_DEGRADED;
            return m_status;
        }
    }

    // Write new metadata to drives
    memset(restore_buffer, 0, SECTOR_SIZE);
    restore_buffer[TIMESTAMP_INDEX] = m_metadata.m_timestamp;
    restore_buffer[FAILED_DRIVE_INDEX] = -1;

    // Writing metadata to replaced drive failed
    if(m_dev->m_Write(m_metadata.m_failed_drive_i, m_metadata_sector, restore_buffer, 1) != 1) {
        m_status = RAID_DEGRADED;
        return m_status;
    }

    for (int dev_i = 0; dev_i < m_dev->m_Devices; dev_i++) {
        if(dev_i == m_metadata.m_failed_drive_i)
            continue;
        if(m_dev->m_Write(dev_i, m_metadata_sector, restore_buffer, 1) != 1) {
            m_status = RAID_DEGRADED;
            m_metadata.m_failed_drive_i = dev_i;
            return m_status;
        }
    }

    m_metadata.m_failed_drive_i = -1;
    m_status = RAID_OK;
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
        raid_sector_to_physical(raid_i, drive_i, drive_sector_i, parity_drive_i);

        // Try read data from "FAIL" drive - use parity
        if (m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i == drive_i) {
            if (xor_read_without_sector(static_cast<int *>(data), drive_i, drive_sector_i) >= 0) {
                // Reading using parity failed, 2+ drives failed, raid failed
                m_status = RAID_FAILED;
                return false;
            }
        }

        // Try read data from "OK" drive in degraded state
        else if (m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i != drive_i) {
            if (m_dev->m_Read(drive_i, drive_sector_i, data, 1) != 1) {
                // Current drive failed in addition to other degraded drive
                m_status = RAID_FAILED;
                return false;
            }
        }

        // RAID must be RAID_OK, read
        else if (m_dev->m_Read(drive_i, drive_sector_i, data, 1) != 1) {
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
        int sector_i = 0;
        int parity_drive_i = 0;
        raid_sector_to_physical(raid_i, drive_i, sector_i, parity_drive_i);

        // Try write data to "FAIL" drive -> only change stripe parity so the "newly
        // written" dead sector data can be recalculated from new parity + other good sectors
        if (m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i == drive_i) {
            // Calculate new parity sector of another "OK" drive
            INT_SECTOR_BUFFER(new_parity_buffer) = {};

            if (xor_get_parity_supplement_dead_sector(new_parity_buffer, parity_drive_i, drive_i,
                                                      static_cast<const int *>(data), sector_i) >= 0) {
                // Calculating new parity failed, 2+ drives failed, raid failed
                m_status = RAID_FAILED;
                return false;
            }

            // Write newly calculated parity to "OK" drive
            if (m_dev->m_Write(parity_drive_i, sector_i, new_parity_buffer, 1) != 1) {
                // Writing parity failed, 2+ drives failed, raid failed
                m_status = RAID_FAILED;
                return false;
            }
        }

        // Try write data to "OK" drive in degraded state -> before writing the new data, recalculate
        else if (m_status == RAID_DEGRADED && m_metadata.m_failed_drive_i != drive_i) {
            // Parity is on dead drive, just write data
            if (m_metadata.m_failed_drive_i == parity_drive_i) {
                if (m_dev->m_Write(drive_i, sector_i, data, 1) != 1) {
                    // Current drive failed in addition to other degraded drive
                    m_status = RAID_FAILED;
                    return false;
                }
            }
            // Parity is among "OK" drives, get original data before calculating new parity
            else {
                // Calculate data on dead drive before changing parity
                INT_SECTOR_BUFFER(dead_drive_data) = {};
                if (xor_read_without_sector(dead_drive_data, m_metadata.m_failed_drive_i, sector_i) >= 0) {
                    m_status = RAID_FAILED;
                    return false;
                }

                // Write new data to OK sector
                if (m_dev->m_Write(drive_i, sector_i, data, 1) != 1) {
                    // Current drive failed in addition to other degraded drive
                    m_status = RAID_FAILED;
                    return false;
                }

                // Calculate new parity sector of another "OK" drive
                INT_SECTOR_BUFFER(new_parity_buffer) = {};
                if (xor_get_parity_supplement_dead_sector(new_parity_buffer, parity_drive_i,
                                                          m_metadata.m_failed_drive_i, dead_drive_data,
                                                          sector_i) >= 0) {
                    // Calculating new parity failed, 2+ drives failed, raid failed
                    m_status = RAID_FAILED;
                    return false;
                }

                // Write newly calculated parity to "OK" drive
                if (m_dev->m_Write(parity_drive_i, sector_i, new_parity_buffer, 1) != 1) {
                    // Writing parity failed, 2+ drives failed, raid failed
                    m_status = RAID_FAILED;
                    return false;
                }
            }
        }

        // RAID must be RAID_OK, write new data and then write new parity
        else if (m_status == RAID_OK) {
            if (m_dev->m_Write(drive_i, sector_i, data, 1) != 1) {
                // Data drive failed, set raid to degraded state
                m_status = RAID_DEGRADED;
                m_metadata.m_failed_drive_i = drive_i;
                // Repeat write in degraded state
                raid_i--;
                continue;
            }

            // Recalculate parity sector
            INT_SECTOR_BUFFER(new_parity_buffer) = {};
            int failed_drive = -1;

            if ((failed_drive = xor_read_without_sector(new_parity_buffer, parity_drive_i, sector_i)) >= 0) {
                // Data drive failed, set raid to degraded state
                m_status = RAID_DEGRADED;
                m_metadata.m_failed_drive_i = failed_drive;
                // Repeat write in degraded state
                raid_i--;
                continue;
            }

            // Write recalculated parity sector
            if (m_dev->m_Write(parity_drive_i, sector_i, new_parity_buffer, 1) != 1) {
                // Parity drive failed, set raid to degraded state
                m_status = RAID_DEGRADED;
                m_metadata.m_failed_drive_i = parity_drive_i;
                // Repeat write in degraded state
                raid_i--;
                continue;
            }
        }

        // Increment buffer pointer
        data += SECTOR_SIZE;
    }

    return true;
}

bool CRaidVolume::validate_t_blk_dev(const TBlkDev &dev) {
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

void CRaidVolume::raid_sector_to_physical(const int raid_sector, int &drive_i, int &drive_sector_i,
                                          int &parity_drive_i) const {
    const int mod = m_dev->m_Devices;

    const int skipped_parities = 1 + (int) (raid_sector / mod) + (raid_sector/(mod * (mod-1)));
    const int phys_sector = raid_sector + skipped_parities;

    drive_i = phys_sector % mod;
    drive_sector_i = phys_sector / mod;
    parity_drive_i = (phys_sector / mod) % mod;
}

void CRaidVolume::clear_raid_volume_data() {
    // Free & reset heap variables
    delete m_dev;
    m_dev = nullptr;

    // Reset stack variables
    m_metadata_sector = 0;
    m_metadata = {};
    m_status = RAID_STOPPED;
    m_raid_size = 0;
}

inline void CRaidVolume::xor_int_buffers(INT_SECTOR_BUFFER(out_buffer), const INT_SECTOR_BUFFER(in_buffer)) {
    for (int i = 0; i < (SECTOR_SIZE / sizeof(int)); i++) {
        out_buffer[i] = out_buffer[i] ^ in_buffer[i];
    }
}

inline int CRaidVolume::xor_read_without_sector(
    INT_SECTOR_BUFFER(out_buffer), const int dead_drive_i, const int sector_i) const {
    // Create next buffer for newly read sectors
    INT_SECTOR_BUFFER(next_buffer);

    // Clear read and next buffer
    memset(out_buffer, 0, SECTOR_SIZE);
    memset(next_buffer, 0, SECTOR_SIZE);

    for (int drive_i = 0; drive_i < m_dev->m_Devices; drive_i++) {
        if (drive_i == dead_drive_i)
            continue;
        if (m_dev->m_Read(drive_i, sector_i, next_buffer, 1) != 1)
            return drive_i;
        xor_int_buffers(out_buffer, next_buffer);
    }
    return -1;
}

inline int CRaidVolume::xor_get_parity_supplement_dead_sector(
    INT_SECTOR_BUFFER(out_buffer), const int parity_drive_i, const int dead_drive_i,
    const INT_SECTOR_BUFFER(dead_drive_supplement_buffer), const int sector_i) const {
    // Create next buffer for newly read sectors
    INT_SECTOR_BUFFER(next_buffer);

    // Clear read and next buffer
    memset(out_buffer, 0, SECTOR_SIZE);
    memset(next_buffer, 0, SECTOR_SIZE);

    for (int drive_i = 0; drive_i < m_dev->m_Devices; drive_i++) {
        if (drive_i == parity_drive_i)
            continue;
        // Supplement dead drive data by provided buffer & xor
        if (drive_i == dead_drive_i) {
            xor_int_buffers(out_buffer, dead_drive_supplement_buffer);
            continue;
        }
        // Xor out buffer with newly read next buffer
        if (m_dev->m_Read(drive_i, sector_i, next_buffer, 1) != 1)
            return drive_i;
        xor_int_buffers(out_buffer, next_buffer);
    }
    return -1;
}


#ifndef __PROGTEST__

#include "custom.inc"

#endif /* __PROGTEST__ */