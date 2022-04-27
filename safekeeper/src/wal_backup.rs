
use std::{path::PathBuf};

use tokio::fs;
use tokio::runtime::{Builder, Runtime};

use lazy_static::lazy_static;
use anyhow::{Result, ensure};

use tokio::sync::watch::Receiver;
use utils::{
    lsn::Lsn,
    zid::{ZTenantTimelineId},
};

use tracing::*;

use crate::remote_storage::{LocalFs, RemoteStorage};
use crate::{broker, SafeKeeperConf};

lazy_static! {
    static ref BACKUP_RUNTIME: Runtime = {
        Builder::new_multi_thread()
            .worker_threads(16)
            .enable_all()
            .build()
            .unwrap()
    };
}

const BACKUP_ELECTION_PATH: &str = "WAL_BACKUP";

// TODO: there's need to track last upload time to avoid losing partially filled segments
// Situation: compute wrote a log record and went idle (log was backed up to S3)
// SK got replaced, compute work up wrote log recrods enough to fill up the segment, 
// we are going to upload the segment but writes before replacement will be missing

#[derive(Clone)]
pub struct WalBackup {
    
    // TODO: Can't figure out how to properly store this
    _remote_storage : &LocalFs,
 
    conf: SafeKeeperConf,

    backup_lsn : Lsn,

    // etcd lease id, used for leader election
    lease_id : Option<i64>,
    timeline_id: ZTenantTimelineId, 
}

async fn detect_task(
    mut backup: WalBackup,
    conf: SafeKeeperConf,
    mut segment_complete: Receiver<(Lsn, Lsn)>,
    mut lsn_durable: Receiver<Lsn>, ) {

    loop {
        if let Some(lease_id) = backup.lease_id {
            broker::become_leader(lease_id, BACKUP_ELECTION_PATH.to_string(), &backup.timeline_id, conf.clone()).await.ok();
        }

        // TODO: can this result in us missing multiple events fired in a rapid succession?
        while segment_complete.changed().await.is_ok() {
            let segment = *segment_complete.borrow();
            let mut commit_lsn = Lsn(0);
            warn!("Woken Up for segment backup start {}, end {}", segment.0, segment.1);

            // TODO: Make this spin optional
            // if ! segment.1 is durable {
            while lsn_durable.changed().await.is_ok() {
                commit_lsn = *lsn_durable.borrow();

                if segment.1 <= commit_lsn {
                    warn!("Woken up for lsn {} is durable, not waiting anymore", commit_lsn);

                    break;
                }

                warn!("Woken up for lsn {} is durable, will retry waiting", commit_lsn);
            }

            // This line assumes the previous task should have updated backup LSN correctly
            backup.backup_segments(backup.backup_lsn, commit_lsn).await.ok();
        }
    }
}


#[allow(dead_code)]
impl WalBackup {
    pub fn create(remote_storage: LocalFs, conf: &SafeKeeperConf, timeline_id: &ZTenantTimelineId, segment_complete: Receiver<(Lsn, Lsn)>, lsn_durable: Receiver<Lsn>) -> Self {
        warn!("Backup service is created");

        let lease =
        if conf.broker_endpoints.is_some() {
            Some(broker::get_lease(conf))
        } else {
            None
        };

        let backup = Self {_remote_storage : &remote_storage, backup_lsn : Lsn::MAX , lease_id: lease, timeline_id: *timeline_id, conf: conf};

        if let Some(lease_id) = lease {
            // TODO: antons how to do it in this function???
            // ensure!( conf.broker_endpoints.is_none(););

            BACKUP_RUNTIME.spawn(broker::lease_keep_alive(lease_id, conf.clone()));
        }

        BACKUP_RUNTIME.spawn(detect_task(backup, conf.clone(), segment_complete, lsn_durable));

        return backup;
    }

    pub fn restore(remote_storage: LocalFs, conf: &SafeKeeperConf, timeline_id: &ZTenantTimelineId, segment_complete: Receiver<(Lsn, Lsn)>, lsn_durable: Receiver<Lsn>) -> Self {
        warn!("Backup service is restored");

        let mut backup = WalBackup::create(remote_storage, conf, timeline_id, segment_complete, lsn_durable);
        backup.upload_missing_segments(Lsn(0)).ok();
        return backup;
    }

    // TODO: enqueue segments between current S3 lsn and current commit LSN
    fn upload_missing_segments(&mut self, start_lsn: Lsn) -> Result<()> {
        self.backup_lsn = start_lsn;
        Ok(())
    }

    pub async fn backup_segments(&mut self, segment_start_lsn: Lsn, segment_end_lsn: Lsn) -> Result<()> {

        for s in get_segments(segment_start_lsn, segment_end_lsn) {


            BACKUP_RUNTIME.block_on(self.backup_single_segment(s))?;

            self.backup_single_segment(s)?;

            // This implementation assumes upload in order
            ensure!(self.backup_lsn >= segment_start_lsn || self.backup_lsn == Lsn::MAX);

            if self.backup_lsn == s.start_lsn {
                self.backup_lsn = s.end_lsn;
            }
        }

        Ok(())
    }

    async fn backup_single_segment(&self, seg: Seg) -> Result<()> {
    
        let mut source_name = self.conf.timeline_dir(&self.timeline_id);
        source_name.push(get_seg_name(seg.seg_no));

        let mut dest_name = PathBuf::from(format!("{}/{}", self.timeline_id.tenant_id, self.timeline_id.timeline_id));
        
        self.conf.timeline_dir(&self.timeline_id);
        dest_name.push(get_seg_name(seg.seg_no));

        warn!("Backup of {} requested for timeline {}", source_name, self.timeline_id.timeline_id);
        let size : usize = seg.end_lsn - seg.start_lsn;

        let source_file = fs::File::open(&source_name).await?;
        self._remote_storage.upload(source_file, size, &dest_name, None).await?;

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Seg {
    seg_no: u64,
    start_lsn: Lsn,
    end_lsn: Lsn,
}

impl Seg {
    pub fn new(seg_no: u64, start_lsn: Lsn, end_lsn: Lsn) -> Self {
        Self { seg_no: seg_no, start_lsn: start_lsn, end_lsn: end_lsn }
    }
}


const SEG_SIZE:u64 = 1024*1024*16;

// TODO, rewrite this function using wal seg size
pub fn get_seg_no(lsn: Lsn) -> u64 {
    u64::from(lsn) / SEG_SIZE
}

pub fn get_seg_lsn(seg:u64) -> Lsn {
    Lsn::from(seg * SEG_SIZE)
}

// TODO rewrite this bogus function with something real
pub fn get_seg_name(seg: u64) -> PathBuf {
    let pb = PathBuf::from(format!("{}", seg));

    pb
}

fn get_segments(start: Lsn, end: Lsn) -> Vec<Seg> {
    let mut res: Vec<Seg> = Vec::new();

    let first_seg = get_seg_no(start);
    let last_seg = get_seg_no(end+1) - 1;
    
    for seg_no in first_seg .. last_seg {
        let start_lsn = get_seg_lsn(seg_no);
        let end_lsn = get_seg_lsn(seg_no+1);

        let s = Seg::new(seg_no, start_lsn, end_lsn);
        res.push(s)
    }

    res
}
