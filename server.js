require('dotenv').config();

const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-app-version'],
}));

app.use(express.json({ limit: '2mb' }));

const PORT = process.env.PORT || 5050;
const REGION = process.env.AWS_REGION;
const BUCKET = process.env.S3_BUCKET;

const PAYOUT_REQUESTS_FILE = path.join(__dirname, 'payout_requests.json');

const readPayoutRequests = () => {
  try {
    if (!fs.existsSync(PAYOUT_REQUESTS_FILE)) {
      fs.writeFileSync(PAYOUT_REQUESTS_FILE, '[]', 'utf8');
    }

    const raw = fs.readFileSync(PAYOUT_REQUESTS_FILE, 'utf8');
    if (!raw.trim()) return [];

    return JSON.parse(raw);
  } catch (error) {
    console.error('READ_PAYOUT_REQUESTS_ERROR:', error);
    return [];
  }
};

const writePayoutRequests = (items) => {
  try {
    fs.writeFileSync(
      PAYOUT_REQUESTS_FILE,
      JSON.stringify(items, null, 2),
      'utf8'
    );
    return true;
  } catch (error) {
    console.error('WRITE_PAYOUT_REQUESTS_ERROR:', error);
    return false;
  }
};

if (!REGION || !BUCKET || !process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
  console.error('Missing required AWS environment variables');
  process.exit(1);
}

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const REQUIRED_UPLOAD_FILES = {
  video: 'video_raw.mp4',
  captureMetadata: 'capture_metadata_v1.json',
  imuMetadata: 'imu_metadata_v1.json',
};

const UPLOAD_COMPLETE_FILE = 'upload_complete.json';
const MIN_DURATION_MS = 30000;
const MULTIPART_EXPIRES_IN_SECONDS = 900;

const MIN_SUPPORTED_APP_VERSION = '1.0.3';

const compareAppVersion = (a, b) => {
  const pa = String(a || '').split('.').map(Number);
  const pb = String(b || '').split('.').map(Number);

  for (let i = 0; i < 3; i += 1) {
    const na = pa[i] || 0;
    const nb = pb[i] || 0;

    if (na > nb) return 1;
    if (na < nb) return -1;
  }

  return 0;
};

const requireSupportedAppVersion = (req, res, next) => {
  const appVersion =
    req.headers['x-app-version'] ||
    req.body?.app_version ||
    req.body?.appVersion ||
    req.body?.client_app_version ||
    req.body?.clientAppVersion;

  if (!appVersion || compareAppVersion(appVersion, MIN_SUPPORTED_APP_VERSION) < 0) {
    return res.status(426).json({
      success: false,
      error: 'APP_UPDATE_REQUIRED',
      code: 'APP_UPDATE_REQUIRED',
      message: 'Please update the Hunter App to continue uploading.',
      minimum_supported_version: MIN_SUPPORTED_APP_VERSION,
      current_version: appVersion || null,
      update_url:
        'https://expo.dev/accounts/origindatalab/projects/new-hunter-app/builds/9f0a480d-7227-4226-b752-73c3515758c0',
    });
  }

  return next();
};

const todayKst = () => {
  const now = new Date();
  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return kst.toISOString().slice(0, 10);
};

const dateKstDaysAgo = (daysAgo) => {
  const now = new Date();
  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  kst.setDate(kst.getDate() - daysAgo);
  return kst.toISOString().slice(0, 10);
};

const isSafeId = (value) => {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{3,120}$/.test(value);
};

const normalizePrefix = (prefix) => {
  if (typeof prefix !== 'string') return null;

  const match = prefix.match(
    /^(?:real\/)?v1\/raw\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/([A-Za-z0-9_-]{3,120})\/([A-Za-z0-9_-]{3,120})\/?$/
  );

  if (!match) return null;

  return {
    date: match[1],
    hunterId: match[2],
    captureId: match[3],
    prefix: `real/v1/raw/${match[1]}/${match[2]}/${match[3]}/`,
  };
};

const normalizeMultipartVideoKey = (body = {}) => {
  const incomingPrefix = body.s3Prefix || body.prefix || null;
  const incomingKey = body.key || null;

  if (incomingPrefix) {
    const parsed = normalizePrefix(incomingPrefix);
    if (!parsed) return null;

    return {
      ...parsed,
      key: `${parsed.prefix}${REQUIRED_UPLOAD_FILES.video}`,
    };
  }

  if (typeof incomingKey === 'string') {
    const match = incomingKey.match(
      /^real\/v1\/raw\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/([A-Za-z0-9_-]{3,120})\/([A-Za-z0-9_-]{3,120})\/video_raw\.mp4$/
    );

    if (!match) return null;

    return {
      date: match[1],
      hunterId: match[2],
      captureId: match[3],
      prefix: `real/v1/raw/${match[1]}/${match[2]}/${match[3]}/`,
      key: incomingKey,
    };
  }

  return null;
};

const normalizeEtagForComplete = (etag) => {
  if (typeof etag !== 'string') return '';

  const cleaned = etag.trim().replace(/^"+|"+$/g, '');

  if (!cleaned) return '';

  return `"${cleaned}"`;
};

const streamToString = (stream) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
  });
};

const readJsonFromS3 = async (key) => {
  const result = await s3.send(
    new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    })
  );

  const body = await streamToString(result.Body);
  return JSON.parse(body);
};

const objectExists = async (key) => {
  try {
    const result = await s3.send(
      new HeadObjectCommand({
        Bucket: BUCKET,
        Key: key,
      })
    );

    return {
      exists: true,
      size: Number(result.ContentLength || 0),
      contentType: result.ContentType || null,
      lastModified: result.LastModified || null,
    };
  } catch (error) {
    if (error?.name === 'NotFound' || error?.$metadata?.httpStatusCode === 404) {
      return {
        exists: false,
        size: 0,
        contentType: null,
        lastModified: null,
      };
    }
    throw error;
  }
};

const listKeysByPrefix = async (prefix) => {
  const keys = [];
  let continuationToken;

  do {
    const result = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    for (const item of result.Contents || []) {
      if (item.Key) {
        keys.push({
          key: item.Key,
          size: Number(item.Size || 0),
          lastModified: item.LastModified || null,
        });
      }
    }

    continuationToken = result.NextContinuationToken;
  } while (continuationToken);

  return keys;
};

const listCaptureMetadataKeys = async () => {
  const keys = [];
  const objects = await listKeysByPrefix('real/v1/raw/');

  for (const item of objects) {
    if (item.key.endsWith('capture_metadata_v1.json')) {
      keys.push(item.key);
    }
  }

  return keys;
};

const getCapturePrefixFromMetadataKey = (key) => {
  return key.replace(/capture_metadata_v1\.json$/, '');
};

const getUploadFileStatus = async (prefix) => {
  const videoKey = `${prefix}${REQUIRED_UPLOAD_FILES.video}`;
  const captureMetadataKey = `${prefix}${REQUIRED_UPLOAD_FILES.captureMetadata}`;
  const imuMetadataKey = `${prefix}${REQUIRED_UPLOAD_FILES.imuMetadata}`;
  const uploadCompleteKey = `${prefix}${UPLOAD_COMPLETE_FILE}`;

  const [video, captureMetadata, imuMetadata, uploadComplete] = await Promise.all([
    objectExists(videoKey),
    objectExists(captureMetadataKey),
    objectExists(imuMetadataKey),
    objectExists(uploadCompleteKey),
  ]);

  const missing = [];
  if (!video.exists || video.size <= 0) missing.push('video_raw.mp4');
  if (!captureMetadata.exists || captureMetadata.size <= 0) missing.push('capture_metadata_v1.json');
  if (!imuMetadata.exists || imuMetadata.size <= 0) missing.push('imu_metadata_v1.json');

  return {
    prefix,
    keys: {
      video: videoKey,
      captureMetadata: captureMetadataKey,
      imuMetadata: imuMetadataKey,
      uploadComplete: uploadCompleteKey,
    },
    files: {
      video,
      captureMetadata,
      imuMetadata,
      uploadComplete,
    },
    missing,
    requiredComplete: missing.length === 0,
    serverMarkedComplete: uploadComplete.exists && uploadComplete.size > 0,
    strictComplete: missing.length === 0 && uploadComplete.exists && uploadComplete.size > 0,
  };
};

const extractHunterId = (metadata, key) => {
  let metadataHunterId =
    metadata.hunter_id ||
    metadata.hunterId ||
    metadata.hunter?.hunter_id ||
    metadata.hunter?.hunterId ||
    metadata.hunter_profile?.hunter_id ||
    metadata.hunter_profile?.hunterId;

  if (!metadataHunterId) {
    const match = key.match(/real\/v1\/raw\/\d{4}-\d{2}-\d{2}\/(HTR-[^\/]+)\//);
    if (match) metadataHunterId = match[1];
  }

  return metadataHunterId;
};

const extractCaptureDate = (metadata, key) => {
  const raw =
    metadata.capture_date ||
    metadata.recording_date ||
    metadata.created_date ||
    metadata.created_at ||
    metadata.created_at_iso ||
    metadata.saved_at_utc ||
    metadata.recording_started_at ||
    metadata.recording_started_at_utc ||
    metadata.recordingStartTime ||
    metadata.session?.capture_date_utc ||
    '';

  if (typeof raw === 'string' && raw.length >= 10) {
    return raw.slice(0, 10);
  }

  const match = key.match(/real\/v1\/raw\/(\d{4}-\d{2}-\d{2})\//);
  return match ? match[1] : null;
};

const getDurationMinutes = (metadata) => {
  const ms =
    Number(metadata.video_actual_duration_ms) ||
    Number(metadata.app_recording_duration_ms) ||
    Number(metadata.video_duration_ms) ||
    Number(metadata.duration_ms) ||
    Number(metadata.recording_duration_ms) ||
    Number(metadata.timing_quality?.recording_elapsed_ms) ||
    0;

  return Math.max(0, ms / 1000 / 60);
};

const getQualityMultiplier = (metadata) => {
  const score =
    Number(metadata.quality_score) ||
    Number(metadata.quality_score_numeric) ||
    Number(metadata.overall_quality_score) ||
    Number(metadata.dataset_quality_score) ||
    Number(metadata.buyer_value_score) ||
    Number(metadata.quality?.score) ||
    70;

  if (score >= 90) return 1.3;
  if (score >= 80) return 1.15;
  if (score >= 60) return 1.0;
  if (score >= 40) return 0.7;
  return 0.4;
};

const calculateEstimatedEarning = (metadata) => {
  const durationMinutes = getDurationMinutes(metadata);
  const baseRatePerMinute = 0.10;
  const qualityMultiplier = getQualityMultiplier(metadata);
  const earning = durationMinutes * baseRatePerMinute * qualityMultiplier;

  return Number(earning.toFixed(2));
};

const getNumber = (value, fallback = 0) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
};

const getAutoReview = (metadata, options = {}) => {
  const uploadComplete = options.uploadComplete === true;
  const uploadMissing = Array.isArray(options.uploadMissing) ? options.uploadMissing : [];
  const durationMinutes = getDurationMinutes(metadata);
  const durationMs = durationMinutes * 60 * 1000;

  const gpsTrackSummary = metadata.gps_track_summary || {};
  const routeDeltaSummary = metadata.route_delta_summary || {};
  const segmentabilityHint = metadata.segmentability_hint || {};
  const gpsCommercialAssessment = metadata.gps_commercial_assessment || {};
  const serverAutoClassification = metadata.server_auto_classification || {};
  const monetizationHint = metadata.monetization_hint || {};
  const segmentSaleability = metadata.segment_saleability || {};

  const totalDistanceM = getNumber(
    gpsTrackSummary.total_distance_m ?? routeDeltaSummary.start_to_end_distance_m,
    0
  );

  const movingPointCount = getNumber(gpsTrackSummary.moving_point_count, 0);
  const gpsPointCount = getNumber(
    gpsTrackSummary.point_count ?? metadata.location?.gps_track_point_count,
    0
  );

  const duplicateRiskScore = getNumber(
    metadata.duplicate_risk_score ??
    metadata.server_inference?.commercialization?.duplicate_risk_score,
    0
  );

  const motionIntensityScore = getNumber(metadata.motion_intensity_score, 0);
  const qualityScore = getNumber(metadata.quality_score_numeric ?? metadata.buyer_value_score, 0);

  const darkSceneRisk =
    metadata.dark_scene_risk ||
    monetizationHint.dark_scene_risk ||
    serverAutoClassification.dark_scene_risk ||
    null;

  const lowSaleProbability =
    metadata.low_sale_probability === true ||
    monetizationHint.low_sale_probability === true ||
    serverAutoClassification.low_sale_probability === true;

  const saleabilityTier =
    metadata.saleability_tier ||
    monetizationHint.saleability_tier ||
    serverAutoClassification.saleability_tier ||
    null;

  const reasons = [];
  const warnings = [];

  if (!uploadComplete) {
    reasons.push('INCOMPLETE_UPLOAD');
  }

  for (const missing of uploadMissing) {
    reasons.push(`MISSING_${missing.toUpperCase().replace(/[^A-Z0-9]+/g, '_')}`);
  }

  if (!durationMs || durationMs <= 0) {
    reasons.push('MISSING_OR_ZERO_DURATION');
  } else if (durationMs < MIN_DURATION_MS) {
    reasons.push('UNDER_3_MINUTES');
  }

  if (gpsCommercialAssessment.gps_reject_recommended === true) {
    warnings.push('GPS_REJECT_RECOMMENDED');
  }

  if (segmentabilityHint.reject_segment_candidate === true) {
    warnings.push('SEGMENT_REJECT_RECOMMENDED');
  }

  if (segmentSaleability.reject_segment_candidate === true) {
    warnings.push('SEGMENT_SALEABILITY_REJECT');
  }

  if (gpsPointCount > 0 && gpsPointCount < 3) {
    reasons.push('INSUFFICIENT_GPS_POINTS');
  }

  if (totalDistanceM < 5 && movingPointCount === 0) {
    reasons.push('STATIONARY_OR_NO_MOVEMENT');
  }

  if (
    metadata.capture_motion_context === 'stationary' ||
    metadata.estimated_travel_direction === 'stationary' ||
    metadata.session_pattern_type === 'mostly_static' ||
    metadata.stop_go_pattern === 'mostly_stopped'
  ) {
    warnings.push('STATIC_MOTION_PATTERN');
  }

  if (duplicateRiskScore >= 90) {
    reasons.push('HIGH_DUPLICATE_RISK');
  } else if (duplicateRiskScore >= 75) {
    warnings.push('DUPLICATE_RISK_REVIEW');
  }

  if (darkSceneRisk === 'high' && lowSaleProbability) {
    warnings.push('DARK_LOW_SALE_PROBABILITY');
  } else if (darkSceneRisk === 'high') {
    warnings.push('DARK_SCENE_REVIEW');
  }

  if (saleabilityTier === 'low' || lowSaleProbability) {
    warnings.push('LOW_SALEABILITY');
  }

  if (metadata.capture_interrupted === true || metadata.session?.capture_interrupted === true) {
    warnings.push('CAPTURE_INTERRUPTED');
  }

  if (metadata.capture_risk_flags?.imu_sampling_low === true) {
    warnings.push('IMU_SAMPLING_LOW');
  }

  if (motionIntensityScore > 0 && motionIntensityScore < 0.01) {
    warnings.push('VERY_LOW_MOTION_INTENSITY');
  }

  if (qualityScore > 0 && qualityScore < 40) {
    warnings.push('LOW_QUALITY_SCORE');
  } else if (qualityScore > 0 && qualityScore < 60) {
    warnings.push('QUALITY_SCORE_REVIEW');
  }

  const uniqueReasons = [...new Set(reasons)];
  const uniqueWarnings = [...new Set(warnings)];

  if (uniqueReasons.length > 0) {
    return {
      status: 'REJECT',
      payable: false,
      review_required: false,
      reasons: uniqueReasons,
      warnings: uniqueWarnings,
      quality_bucket: 'reject',
    };
  }

  if (uniqueWarnings.length > 0) {
    return {
      status: 'GOOD_PENDING_REVIEW',
      payable: true,
      review_required: true,
      reasons: [],
      warnings: uniqueWarnings,
      quality_bucket: 'warning_pass',
    };
  }

  return {
    status: 'GOOD_PENDING_REVIEW',
    payable: true,
    review_required: false,
    reasons: [],
    warnings: [],
    quality_bucket: 'good_candidate',
  };
};

const calculatePayableEarning = (metadata, review) => {
  if (!review || !review.payable) return 0;
  return calculateEstimatedEarning(metadata);
};

const extractCaptureSortTime = (metadata, key) => {
  const raw =
    metadata.recording_started_at ||
    metadata.recording_started_at_utc ||
    metadata.recordingStartTime ||
    metadata.created_at ||
    metadata.created_at_iso ||
    metadata.saved_at_utc ||
    metadata.capture_started_at ||
    metadata.session?.recording_started_at ||
    metadata.session?.recording_started_at_utc ||
    '';

  if (typeof raw === 'string' && raw) {
    const t = new Date(raw).getTime();
    if (Number.isFinite(t)) return t;
  }

  const match = String(key || '').match(/capture_(\d{8})T(\d{6})Z/);
  if (match) {
    const y = match[1].slice(0, 4);
    const m = match[1].slice(4, 6);
    const d = match[1].slice(6, 8);
    const hh = match[2].slice(0, 2);
    const mm = match[2].slice(2, 4);
    const ss = match[2].slice(4, 6);
    return new Date(`${y}-${m}-${d}T${hh}:${mm}:${ss}Z`).getTime();
  }

  return 0;
};

const buildSessionId = ({ hunterId, date, startedAtMs }) => {
  const d = startedAtMs ? new Date(startedAtMs) : new Date();
  const stamp = d.toISOString()
    .replace(/[-:]/g, '')
    .replace(/\.\d{3}Z$/, 'Z');

  return `SESSION-${hunterId}-${date}-${stamp}`;
};

const groupReviewItemsIntoSessions = (items, gapMinutes = 5) => {
  const sorted = [...items].sort((a, b) => {
    if (a.hunter_id !== b.hunter_id) {
      return String(a.hunter_id).localeCompare(String(b.hunter_id));
    }

    if (a.capture_date !== b.capture_date) {
      return String(a.capture_date || '').localeCompare(String(b.capture_date || ''));
    }

    return Number(a.sort_time_ms || 0) - Number(b.sort_time_ms || 0);
  });

  const sessions = [];
  let current = null;
  const maxGapMs = gapMinutes * 60 * 1000;

  for (const item of sorted) {
    const startedAtMs = Number(item.sort_time_ms || 0);
    const itemDurationMs = Number(item.duration_minutes || 0) * 60 * 1000;

    const shouldStartNew =
      !current ||
      current.hunter_id !== item.hunter_id ||
      current.capture_date !== item.capture_date ||
      (
        current.last_end_ms > 0 &&
        startedAtMs > 0 &&
        startedAtMs - current.last_end_ms > maxGapMs
      );

    if (shouldStartNew) {
      current = {
        session_id: buildSessionId({
          hunterId: item.hunter_id,
          date: item.capture_date || 'unknown-date',
          startedAtMs,
        }),
        hunter_id: item.hunter_id,
        hunter: item.hunter,
        capture_date: item.capture_date,
        started_at_ms: startedAtMs,
        ended_at_ms: startedAtMs + itemDurationMs,
        last_end_ms: startedAtMs + itemDurationMs,
        part_count: 0,
        total_duration_minutes: 0,
        payable_duration_minutes: 0,
        estimated_earning_usd: 0,
        reject_reasons: [],
        review_warnings: [],
        parts: [],
      };

      sessions.push(current);
    }

    current.parts.push({
      ...item,
      part_index: current.parts.length + 1,
    });

    current.part_count += 1;
    current.total_duration_minutes += Number(item.duration_minutes || 0);

    if (item.payable) {
      current.payable_duration_minutes += Number(item.duration_minutes || 0);
      current.estimated_earning_usd += Number(item.estimated_earning_usd || 0);
    }

    current.reject_reasons.push(...(item.reject_reasons || []));
    current.review_warnings.push(...(item.review_warnings || []));

    const endMs = startedAtMs + itemDurationMs;
    if (endMs > current.ended_at_ms) current.ended_at_ms = endMs;
    if (endMs > current.last_end_ms) current.last_end_ms = endMs;
  }

  return sessions.map((session) => {
    const hasReject = session.parts.some((part) => part.status === 'REJECT');
    const hasPayable = session.parts.some((part) => part.payable);

    let status = 'GOOD_PENDING_REVIEW';
    if (!hasPayable && hasReject) status = 'REJECT';
    else if (hasReject && hasPayable) status = 'PARTIAL_PASS';

    return {
      ...session,
      status,
      total_duration_minutes: Number(session.total_duration_minutes.toFixed(2)),
      payable_duration_minutes: Number(session.payable_duration_minutes.toFixed(2)),
      estimated_earning_usd: Number(session.estimated_earning_usd.toFixed(2)),
      reject_reasons: [...new Set(session.reject_reasons)],
      review_warnings: [...new Set(session.review_warnings)],
      started_at: session.started_at_ms ? new Date(session.started_at_ms).toISOString() : '',
      ended_at: session.ended_at_ms ? new Date(session.ended_at_ms).toISOString() : '',
    };
  }).sort((a, b) => Number(b.started_at_ms || 0) - Number(a.started_at_ms || 0));
};

const getTierByEarnings = (totalEarnings) => {
  if (totalEarnings >= 50) return 'DIAMOND HUNTER';
  if (totalEarnings >= 20) return 'GOLD HUNTER';
  if (totalEarnings >= 5) return 'SILVER HUNTER';
  return 'BRONZE HUNTER';
};

const buildCapturePrefix = ({ date, hunterId, captureId }) => {
  return `real/v1/raw/${date}/${hunterId}/${captureId}/`;
};

const createServerUploadCompleteMarker = async ({ prefix, hunterId, captureId, uploadStatus, metadata, review }) => {
  const marker = {
    success: true,
    server_verified: true,
    completed_by: 's3-presign-server',
    completed_at: new Date().toISOString(),
    hunter_id: hunterId,
    capture_id: captureId,
    s3_prefix: prefix,
    required_files: REQUIRED_UPLOAD_FILES,
    file_status: uploadStatus.files,
    duration_minutes: Number(getDurationMinutes(metadata).toFixed(2)),
    review_status: review.status,
    payable: review.payable,
    reject_reasons: review.reasons,
    review_warnings: review.warnings,

    upload_transport_complete: true,
    dataset_segmentation_requested: true,
    dataset_segment_mode: 'server_only_after_raw_complete',
  };

  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${prefix}${UPLOAD_COMPLETE_FILE}`,
      ContentType: 'application/json',
      Body: JSON.stringify(marker, null, 2),
    })
  );

  return marker;
};
app.get('/admin/payouts', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Payout Dashboard</title>
  <style>
    body {
      margin: 0;
      padding: 16px;
      font-family: Arial, sans-serif;
      background: #f5f6f8;
      color: #111827;
      font-size: 13px;
    }

    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
    }

    h2 {
      margin: 0;
      font-size: 22px;
    }

    .navBtn {
      border: 0;
      background: #111827;
      color: white;
      padding: 9px 14px;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 800;
    }

    .cards {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
      margin-bottom: 14px;
    }

    .card {
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      padding: 14px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

    .cardTitle {
      color: #6b7280;
      font-size: 12px;
      font-weight: 800;
      margin-bottom: 8px;
    }

    .cardValue {
      font-size: 24px;
      font-weight: 900;
    }

    .tableWrap {
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      overflow: auto;
      box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

    table {
      width: 100%;
      min-width: 1000px;
      border-collapse: collapse;
      table-layout: fixed;
    }

    th {
      position: sticky;
      top: 0;
      background: #111827;
      color: white;
      padding: 10px 8px;
      text-align: left;
      font-size: 12px;
      white-space: nowrap;
    }

    td {
      border-bottom: 1px solid #e5e7eb;
      padding: 9px 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      background: white;
    }

    tr:hover td {
      background: #f9fafb;
    }

    .col-no { width: 50px; text-align: center; }
    .col-hunter { width: 140px; }
    .col-phone { width: 125px; }
    .col-amount { width: 100px; text-align: right; }
    .col-status { width: 100px; }
    .col-date { width: 150px; }
    .col-note { width: 260px; white-space: normal; font-size: 12px; color: #374151; }
    .col-action { width: 120px; text-align: center; }

    .statusPending {
      color: #d97706;
      font-weight: 900;
    }

    .statusPaid {
      color: #059669;
      font-weight: 900;
    }

    .statusReject {
      color: #dc2626;
      font-weight: 900;
    }

    .smallBtn {
      border: 0;
      background: #2563eb;
      color: white;
      padding: 7px 10px;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 800;
    }

    .muted {
      color: #6b7280;
    }

.payPendingBtn {
  border: 0;
  background: #dc2626;
  color: white;
  padding: 7px 10px;
  border-radius: 8px;
  cursor: pointer;
  font-weight: 800;
}

.paidDoneBtn {
  border: 0;
  background: #2563eb;
  color: white;
  padding: 7px 10px;
  border-radius: 8px;
  font-weight: 800;
}

.linkBtn {
  border: 0;
  background: transparent;
  color: #2563eb;
  font-weight: 800;
  cursor: pointer;
  padding: 0;
}

.linkBtn:hover {
  text-decoration: underline;
}

.popupBg {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.45);
  z-index: 50;
}

.popupBox {
  background: white;
  width: 720px;
  max-width: calc(100vw - 40px);
  margin: 60px auto;
  border-radius: 12px;
  padding: 18px;
  box-shadow: 0 20px 50px rgba(0,0,0,0.25);
}

.popupClose {
  float: right;
  border: 0;
  background: #111827;
  color: white;
  border-radius: 8px;
  padding: 7px 10px;
  cursor: pointer;
}

.popupGrid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 10px;
  margin: 14px 0;
}

.popupMini {
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 10px;
}

  </style>
</head>
<body>

  <div class="topbar">
    <h2>Payout Dashboard</h2>
    <button class="navBtn" onclick="location.href='/admin/review'">검수 페이지</button>
  </div>

  <div class="cards">
    <div class="card">
      <div class="cardTitle">TOTAL REQUESTS</div>
      <div id="totalRequests" class="cardValue">-</div>
    </div>
    <div class="card">
      <div class="cardTitle">PENDING</div>
      <div id="pendingAmount" class="cardValue">$0.00</div>
    </div>
    <div class="card">
      <div class="cardTitle">PAID</div>
      <div id="paidAmount" class="cardValue">$0.00</div>
    </div>
    <div class="card">
      <div class="cardTitle">HUNTERS</div>
      <div id="hunterCount" class="cardValue">-</div>
    </div>
  </div>

  <div class="tableWrap">
    <table>
   <thead>
  <tr>
  <th>No</th>
  <th>Hunter ID</th>
  <th>Nickname</th>
  <th>Referrer</th>
  <th>Phone</th>
  <th>Country</th>
  <th>Total Min</th>
  <th>Payable Min</th>
  <th>Uploads</th>
  <th>Reject</th>
  <th>Total</th>
  <th>Available</th>
</tr>
</thead>
<tbody id="rows"></tbody>
    </table>
 </div>

<div id="popupBg" class="popupBg">
  <div class="popupBox">
    <button class="popupClose" onclick="closeHunterPopup()">닫기</button>

    <h2 id="popupTitle">Hunter Detail</h2>

    <div id="popupBody"></div>
  </div>
</div>

<script>
function safeText(v) {
  if (v === null || v === undefined) return '';
  return String(v);
}

function money(v) {
  return '$' + Number(v || 0).toFixed(2);
}

function formatDate(dateStr) {
  if (!dateStr) return '';

  const d = new Date(dateStr);

  if (Number.isNaN(d.getTime())) {
    return dateStr;
  }

  return d.getFullYear() + '-' +
    String(d.getMonth() + 1).padStart(2, '0') + '-' +
    String(d.getDate()).padStart(2, '0') + ' ' +
    String(d.getHours()).padStart(2, '0') + ':' +
    String(d.getMinutes()).padStart(2, '0');
}

function statusClass(status) {
  if (status === 'PAID') return 'statusPaid';
  if (status === 'REJECT' || status === 'REJECTED') return 'statusReject';
  return 'statusPending';
}

function getPayoutButton(request) {
  const status = safeText(request.status || '').toLowerCase();

  if (!request || !request.request_id) {
    return '<span class="muted">요청 없음</span>';
  }

  if (status === 'paid') {
    return '<button class="paidDoneBtn">지급 완료</button>';
  }

  return '<button class="payPendingBtn" onclick="alert(\\'수동 지급 후 paid 처리 API 연결 예정\\')">지급 예정</button>';
}

async function loadSummary() {
  try {
    const res = await fetch('/admin/payout-summary');
    const data = await res.json();

    if (!data.success) return;

    document.getElementById('totalRequests').innerText =
      data.total_requests ?? data.totalRequests ?? data.total ?? '-';

    document.getElementById('pendingAmount').innerText =
      money(data.pending_amount_usd ?? data.pendingAmountUsd ?? data.pending ?? 0);

    document.getElementById('paidAmount').innerText =
      money(data.paid_amount_usd ?? data.paidAmountUsd ?? data.paid ?? 0);

   const huntersValue =
  data.hunter_count ??
  data.hunterCount ??
  data.hunters ??
  [];

document.getElementById('hunterCount').innerText =
  Array.isArray(huntersValue)
    ? huntersValue.length
    : huntersValue;
  } catch (e) {
    console.error(e);
  }
}

async function loadHunters() {
  const rows = document.getElementById('rows');

  rows.innerHTML =
    '<tr><td colspan="10" class="muted">Loading...</td></tr>';

  try {
    const res = await fetch('/admin/payout-summary');
    const data = await res.json();

    if (!data.success) {
      rows.innerHTML = '<tr><td colspan="10">Failed to load hunters</td></tr>';
      return;
    }

    const hunters = data.hunters || [];
    const requests = data.payout_requests || [];

    if (!hunters.length) {
      rows.innerHTML = '<tr><td colspan="10">No hunters</td></tr>';
      return;
    }

    rows.innerHTML = '';

    hunters.forEach(function(hunter, index) {
      const hunterRequests = requests.filter(function(r) {
        return r.hunter_id === hunter.hunter_id;
      });

      const latestRequest = hunterRequests[0] || {};
      const tr = document.createElement('tr');

           tr.innerHTML =
        '<td class="col-no">' + (index + 1) + '</td>' +

        '<td class="col-hunter"><button class="linkBtn">' +
          safeText(hunter.hunter_id) +
        '</button></td>' +

        '<td class="col-hunter"><button class="linkBtn">' +
          safeText(hunter.nickname || '-') +
        '</button></td>' +

        '<td class="col-hunter" title="' + safeText(hunter.recruited_by_hunter_id || '-') + '">' +
          safeText(hunter.recruited_by_hunter_id || '-') +
        '</td>' +

        '<td class="col-phone" title="' + safeText(hunter.phone) + '">' +
          safeText(hunter.phone || '-') +
        '</td>' +

        '<td class="col-status" title="' + safeText(hunter.country) + '">' +
          safeText(hunter.country || '-') +
        '</td>' +

        '<td class="col-amount">' +
          Number(hunter.total_minutes || 0).toFixed(2) +
        '</td>' +

        '<td class="col-amount">' +
          Number(hunter.payable_minutes || 0).toFixed(2) +
        '</td>' +

        '<td class="col-amount">' +
          Number(hunter.total_uploads || 0) +
        '</td>' +

        '<td class="col-amount">' +
          Number(hunter.rejected_uploads || 0) +
        '</td>' +

        '<td class="col-amount">' +
          money(hunter.total_earnings || 0) +
        '</td>' +

        '<td class="col-amount">' +
          money(hunter.available_balance || 0) +
        '</td>';

      const buttons = tr.querySelectorAll('.linkBtn');
      buttons.forEach(function(btn) {
        btn.onclick = function() {
          openHunterPopup(hunter, hunterRequests);
        };
      });

      rows.appendChild(tr);
    });

  } catch (e) {
    console.error(e);
    rows.innerHTML = '<tr><td colspan="10">Error loading hunters</td></tr>';
  }
}
function openHunterPopup(hunter, requests) {

  const bg = document.getElementById('popupBg');
  const title = document.getElementById('popupTitle');
  const body = document.getElementById('popupBody');

  title.innerText =
    safeText(hunter.nickname || '-') +
    ' / ' +
    safeText(hunter.hunter_id);

  let requestRows = '';

  if (!requests.length) {

    requestRows =
      '<tr><td colspan="5">페이아웃 요청 없음</td></tr>';

  } else {

    requests.forEach(function(r) {

      requestRows +=
        '<tr>' +
          '<td>' + money(r.amount || 0) + '</td>' +
          '<td>' + safeText(r.status || '') + '</td>' +
          '<td>' + formatDate(r.created_at || '') + '</td>' +
          '<td>' + formatDate(r.approved_at || '') + '</td>' +
          '<td>' + formatDate(r.paid_at || '') + '</td>' +
        '</tr>';

    });

  }

  body.innerHTML =

    '<div class="popupGrid">' +

      '<div class="popupMini"><b>총 번 금액</b><br>' +
        money(hunter.total_earnings || 0) +
      '</div>' +

      '<div class="popupMini"><b>가용 정산금</b><br>' +
        money(hunter.available_balance || 0) +
      '</div>' +

      '<div class="popupMini"><b>지급 완료</b><br>' +
        money(hunter.paid_total || 0) +
      '</div>' +

      '<div class="popupMini"><b>총 업로드</b><br>' +
        Number(hunter.total_uploads || 0) +
      '</div>' +

      '<div class="popupMini"><b>승인</b><br>' +
        Number(hunter.payable_uploads || 0) +
      '</div>' +

      '<div class="popupMini"><b>리젝</b><br>' +
        Number(hunter.rejected_uploads || 0) +
      '</div>' +

    '</div>' +

    '<h3>페이아웃 내역</h3>' +

    '<table style="width:100%; border-collapse:collapse;">' +

      '<thead>' +
        '<tr>' +
          '<th>Amount</th>' +
          '<th>Status</th>' +
          '<th>Request</th>' +
          '<th>Approved</th>' +
          '<th>Paid</th>' +
        '</tr>' +
      '</thead>' +

      '<tbody>' +
        requestRows +
      '</tbody>' +

    '</table>';

  bg.style.display = 'block';
}

function closeHunterPopup() {
  document.getElementById('popupBg').style.display = 'none';
}

loadSummary();
loadHunters();
</script>

</body>
</html>
  `);
});

app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 's3-presign-server',
    bucket: BUCKET,
    region: REGION,
    upload_complete_mode: 'server_verified_only',
    required_files: REQUIRED_UPLOAD_FILES,
    minimum_duration_minutes: 0.5,
    multipart_upload_enabled: true,
    multipart_endpoints: [
      '/api/v1/s3-multipart/create',
      '/api/v1/s3-multipart/part-url',
      '/api/v1/s3-multipart/complete',
      '/api/v1/s3-multipart/abort',
    ],
    dataset_segment_mode: 'server_only_after_raw_complete',
  });
});

app.get('/api/v1/app-version-policy', (req, res) => {
  res.json({
    success: true,
    platform: 'android',
    minimum_supported_version: '1.0.3',
    latest_version: '1.0.3',
    force_update: true,
    update_url: 'https://expo.dev/accounts/origindatalab/projects/new-hunter-app/builds/9f0a480d-7227-4226-b752-73c3515758c0',
    message: 'A required update is available. Please update the Hunter App.'
  });
});

app.get('/admin/review', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Review Uploads</title>
  <style>
    body {
      margin: 0;
      padding: 16px;
      font-family: Arial, sans-serif;
      background: #f5f6f8;
      color: #111827;
      font-size: 13px;
    }

    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }

    h2 {
      margin: 0;
      font-size: 20px;
    }

    #summary {
      font-weight: 700;
      color: #374151;
    }

    .filters {
      display: flex;
      gap: 8px;
      margin-bottom: 12px;
    }

    .filters button {
      border: 1px solid #d1d5db;
      background: #fff;
      padding: 7px 12px;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 700;
    }

    .filters button:hover {
      background: #eef2ff;
    }

    .tableWrap {
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      overflow: auto;
      box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

      table {
      width: 100%;
      min-width: 1180px;
      border-collapse: collapse;
      table-layout: fixed;
    }

    th {
  position: sticky;
  top: 0;
  background: #111827;
  color: #fff;
  padding: 9px 8px;
  text-align: right;
  font-size: 12px;
  white-space: nowrap;
  z-index: 2;
}

      td {
  border-bottom: 1px solid #e5e7eb;
  padding: 5px 4px;
  vertical-align: top;
  background: #fff;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 11px;
  text-align: right;
}

    tr:hover td {
      background: #f9fafb;
    }

    tr.selected td {
      background: #eef2ff;
    }

     .col-no { width: 36px; text-align: center; }
    .col-uploaded { width: 105px; }
    .col-hunter { width: 95px; }
    .col-nickname { width: 75px; }
    .col-referrer { width: 95px; }
    .col-phone { width: 105px; }
    .col-country { width: 45px; }
    .col-city { width: 60px; }
    .col-duration { width: 48px; text-align: right; }

    .col-status {
      width: 105px;
      white-space: normal;
      word-break: break-word;
      line-height: 1.2;
      font-size: 11px;
    }

    .col-usd { width: 55px; text-align: right; }
    .col-reject { width: 145px; }
    .col-warning { width: 145px; }
    .col-preview { width: 235px; }

    .reason {
      white-space: normal;
      line-height: 1.35;
      font-size: 11px;
      color: #374151;
      max-height: 48px;
      overflow: hidden;
    }

    .statusReject {
      color: #dc2626;
      font-weight: 800;
    }

    .statusApprove {
      color: #059669;
      font-weight: 800;
    }

    .statusHold {
      color: #d97706;
      font-weight: 800;
    }

     .previewBtn {
      border: 0;
      background: #2563eb;
      color: white;
      padding: 4px 8px;
      border-radius: 6px;
      cursor: pointer;
      font-weight: 800;
      margin-bottom: 4px;
      font-size: 11px;
    }

    .previewBtn:hover {
      background: #1d4ed8;
    }

     .closePreviewBtn {
     border: 0;
     background: #6b7280;
     color: white;
     padding: 4px 8px;
     border-radius: 6px;
     cursor: pointer;
     font-weight: 800;
     margin-bottom: 4px;
     font-size: 11px;
   }

    .closePreviewBtn:hover {
      background: #374151;
   }

      .inlinePreview {
  display: none;
  width: 220px;
  height: 124px;
  background: #000;
  border-radius: 8px;
  object-fit: contain;
  aspect-ratio: 16 / 9;
}

.inlinePreview.rotate90 {
  transform: rotate(-90deg);
  transform-origin: center center;
  width: 124px;
  height: 220px;
  margin: -48px 48px;
}

    .inlinePreview.show {
  display: block;
}

.partsRow td {
  background: #f8fafc;
  white-space: normal;
  padding: 10px;
}

.partsBox {
  border: 1px solid #d1d5db;
  border-radius: 10px;
  padding: 10px;
  background: #ffffff;
}

.partItem {
  display: grid;
  grid-template-columns: 40px 70px 150px minmax(0, 1fr);
  gap: 14px;
  align-items: start;
  border-bottom: 1px solid #e5e7eb;
  padding: 8px 0;
  font-size: 11px;
}

.partItem:last-child {
  border-bottom: 0;
}

.partBadge {
  font-weight: 900;
}

.partReject {
  color: #dc2626;
}

.partPass {
  color: #059669;
}

.partBadge {
  font-weight: 900;
  white-space: normal;
  word-break: break-word;
  line-height: 1.3;
}

.partReason {
  white-space: normal;
  word-break: break-word;
  line-height: 1.4;
  color: #111827;
}

.partsBtn {
  border: 0;
  background: #7c3aed;
  color: white;
  padding: 4px 8px;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 800;
  margin-bottom: 4px;
  font-size: 11px;
}
  </style>
</head>
<body>

  <div class="topbar">
    <h2>Review Uploads</h2>
    <div id="summary">Loading...</div>
  </div>

 <div class="filters">
  <button onclick="load('')">ALL</button>
  <button onclick="load('APPROVE')">APPROVE</button>
  <button onclick="load('REJECT')">REJECT</button>
  <button onclick="load('HOLD')">HOLD</button>

<input
  id="quickFilter"
  placeholder="Hunter / Nickname / Referrer search"
  oninput="applyQuickFilter()"
  style="
    margin-left:12px;
    padding:7px 10px;
    border:1px solid #d1d5db;
    border-radius:8px;
    min-width:260px;
    font-weight:700;
  "
/>

  <button
    onclick="location.href='/admin/payouts'"
    style="
      margin-left:16px;
      background:#111827;
      color:white;
      border:1px solid #111827;
    "
  >
    정산 페이지
  </button>
</div>

  <div class="tableWrap">
    <table>
      <thead>
        <tr>
          <th class="col-no">No</th>
          <th class="col-uploaded">Uploaded At</th>
          <th class="col-hunter">Hunter</th>
          <th class="col-nickname">Nickname</th>
          <th class="col-referrer">Referrer</th>
          <th class="col-phone">Phone</th>
          <th class="col-country">Country</th>
          <th class="col-city">City</th>
          <th class="col-duration">Min</th>
          <th class="col-status">Status</th>
          <th class="col-usd">USD</th>
          <th class="col-reject">Reject</th>
          <th class="col-warning">Warning</th>
          <th class="col-preview">Preview / Parts</th>
        </tr>
      </thead>
      <tbody id="rows"></tbody>
    </table>
  </div>

<script>
let selectedRow = null;

function safeText(value) {
  if (value === null || value === undefined) return '';
  return String(value);
}

function extractUploadedAt(item) {
  const source = item.s3_prefix || item.s3_key || item.video_key || '';
  const match = source.match(/capture_(\\d{8})T(\\d{6})Z/);

  if (!match) {
    return item.uploaded_at || item.created_at || item.capture_date || '';
  }

  const y = match[1].slice(0, 4);
  const m = match[1].slice(4, 6);
  const d = match[1].slice(6, 8);
  const hh = match[2].slice(0, 2);
  const mm = match[2].slice(2, 4);

  return y + '-' + m + '-' + d + ' ' + hh + ':' + mm;
}

function getStatusClass(status) {
  if (status === 'REJECT') return 'statusReject';
  if (status === 'HOLD') return 'statusHold';
  return 'statusApprove';
}

async function load(status) {
  const summary = document.getElementById('summary');
  const rows = document.getElementById('rows');

  summary.innerText = 'Loading...';
  rows.innerHTML = '';

  const url = status
  ? '/admin/review-sessions?limit=100&status=' + encodeURIComponent(status)
  : '/admin/review-sessions?limit=100';

  const res = await fetch(url);
  const data = await res.json();

  if (!data.success) {
    summary.innerText = 'Failed to load';
    return;
  }

  const totalPayableMinutes = (data.items || []).reduce(function(sum, item) {
  return sum + Number(item.payable_duration_minutes || 0);
}, 0);

const totalPayableHours = totalPayableMinutes / 60;

summary.innerHTML =
  'Total: ' + data.total +
  ' / Filter: ' + (status || 'ALL') +
  ' / <span style="color:#dc2626; font-weight:900;">Sellable: ' +
  totalPayableHours.toFixed(2) +
  ' hours</span>';

  (data.items || []).forEach(function(item, index) {
    const hunter = item.hunter || {};
    const tr = document.createElement('tr');

    const no = index + 1;
    const uploadedAt = extractUploadedAt(item);
    const hunterId = safeText(item.hunter_id || hunter.hunter_id);
       const nickname = safeText(hunter.nickname);
    const referrer = safeText(
      hunter.recruited_by_hunter_id ||
      hunter.recruitedByHunterId ||
      hunter.referrer_hunter_id ||
      hunter.referrerHunterId ||
      '-'
    );
    const phone = safeText(hunter.phone);
    const country = safeText(hunter.country);
    const city = safeText(hunter.city);
   const duration = Number(item.total_duration_minutes || 0).toFixed(2);
   const statusText = safeText(item.status);
   const usd = Number(item.estimated_earning_usd || 0).toFixed(2);
   const rejectText = safeText((item.reject_reasons || []).join(', '));
   const warningText = safeText((item.review_warnings || []).join(', '));
   const partCount = Number(item.part_count || 0);
   const parts = item.parts || [];
   const firstPart = parts[0] || {};
   const videoId = 'preview_' + index;
   const partsId = 'parts_' + index;

    tr.innerHTML =
      '<td class="col-no">' + no + '</td>' +
      '<td class="col-uploaded" title="' + uploadedAt + '">' + uploadedAt + '</td>' +
      '<td class="col-hunter" title="' + hunterId + '">' + hunterId + '</td>' +
      '<td class="col-nickname" title="' + nickname + '">' + nickname + '</td>' +
      '<td class="col-referrer" title="' + referrer + '">' + referrer + '</td>' +
      '<td class="col-phone" title="' + phone + '">' + phone + '</td>' +
      '<td class="col-country" title="' + country + '">' + country + '</td>' +
      '<td class="col-city" title="' + city + '">' + city + '</td>' +
     '<td class="col-duration" title="Parts: ' + partCount + '">' + duration + '</td>' +
      '<td class="col-status ' + getStatusClass(statusText) + '">' + statusText + '</td>' +
      '<td class="col-usd">$' + usd + '</td>' +
      '<td class="col-reject"><div class="reason" title="' + rejectText + '">' + rejectText + '</div></td>' +
      '<td class="col-warning"><div class="reason" title="' + warningText + '">' + warningText + '</div></td>' +
      '<td class="col-preview">' +
      '<button class="previewBtn">보기</button> ' +
      '<button class="closePreviewBtn">닫기</button> ' +
      '<button class="partsBtn">조각 ' + partCount + '개</button>' +
      '<video id="' + videoId + '" class="inlinePreview rotate90" controls muted></video>' +
    '</td>';

  tr.querySelector('.previewBtn').onclick = function() {
  previewVideo(videoId, firstPart.video_key, tr);
};

tr.querySelector('.closePreviewBtn').onclick = function() {
  closePreview(videoId, tr);
};

tr.querySelector('.partsBtn').onclick = function() {
  togglePartsRow(partsId, tr, parts);
};

rows.appendChild(tr);
applyQuickFilter();
  });
}

async function previewVideo(videoId, videoKey, row) {

  document.querySelectorAll('.inlinePreview').forEach(function(v) {
    if (v.id !== videoId) {
      v.pause();
      v.classList.remove('show');
    }
  });
  if (!videoKey) {
    alert('video_key 없음');
    return;
  }

  if (selectedRow) selectedRow.classList.remove('selected');
  row.classList.add('selected');
  selectedRow = row;

  const video = document.getElementById(videoId);

  if (video.src) {
    video.classList.toggle('show');
    return;
  }

  const res = await fetch('/admin/video-url?key=' + encodeURIComponent(videoKey));
  const data = await res.json();

  if (!data.success || !data.url) {
    alert('영상 URL 생성 실패');
    return;
  }

  video.src = data.url;
  video.classList.add('show');
}

function applyQuickFilter() {
  const q = safeText(document.getElementById('quickFilter')?.value || '')
    .toLowerCase()
    .trim();

  document.querySelectorAll('#rows tr').forEach(function(row) {
    if (row.classList.contains('partsRow')) {
      return;
    }

    const text = row.innerText.toLowerCase();
    row.style.display = !q || text.includes(q) ? '' : 'none';

    const next = row.nextElementSibling;
    if (next && next.classList.contains('partsRow') && row.style.display === 'none') {
      next.style.display = 'none';
    }
  });
}

function closePreview(videoId, row) {
  const video = document.getElementById(videoId);

  if (video) {
    video.pause();
    video.removeAttribute('src');
    video.load();
    video.classList.remove('show');
  }

  if (row) {
    row.classList.remove('selected');
  }

  if (selectedRow === row) {
    selectedRow = null;
  }
}

function togglePartsRow(partsId, row, parts) {
  const existing = document.getElementById(partsId);

  if (existing) {
    existing.remove();
    return;
  }

  const detailRow = document.createElement('tr');
  detailRow.id = partsId;
  detailRow.className = 'partsRow';

  let html = '<td colspan="14"><div class="partsBox">';
  html += '<b>Session Parts</b>';

  if (!parts || !parts.length) {
    html += '<div class="muted">조각 정보 없음</div>';
  } else {
    parts.forEach(function(part) {
      const status = safeText(part.status || '');
      const badgeClass = status === 'REJECT' ? 'partReject' : 'partPass';
      const duration = Number(part.duration_minutes || 0).toFixed(2);
      const reject = safeText((part.reject_reasons || []).join(', '));
      const warning = safeText((part.review_warnings || []).join(', '));

      html +=
        '<div class="partItem">' +
          '<div>#' + Number(part.part_index || 0) + '</div>' +
          '<div>' + duration + ' min</div>' +
          '<div class="partBadge ' + badgeClass + '">' + status + '</div>' +
          '<div class="partReason" title="' + reject + '">' + (reject || warning || '-') +
          '</div>' +
          '<div><button class="previewBtn" onclick="previewVideo(\\'inline_part_' + partsId + '_' + part.part_index + '\\', \\'' + part.video_key + '\\', this.closest(\\'tr\\'))">보기</button></div>' +
        '</div>' +
        '<video id="inline_part_' + partsId + '_' + part.part_index + '" class="inlinePreview rotate90" controls muted></video>';
    });
  }

  html += '</div></td>';

  detailRow.innerHTML = html;
  row.insertAdjacentElement('afterend', detailRow);
}

load('');
</script>

</body>
</html>
`);
});

app.get('/admin/review-uploads', async (req, res) => {
  try {
    const hunterFilter = req.query.hunter_id || req.query.hunterId || null;
    const statusFilter = req.query.status || null;
    const limit = Math.min(Number(req.query.limit || 200), 1000);

    const keys = await listCaptureMetadataKeys();
    const items = [];

    for (const key of keys) {
      try {
        const prefix = getCapturePrefixFromMetadataKey(key);
        const uploadStatus = await getUploadFileStatus(prefix);
        const uploadComplete = uploadStatus.strictComplete;

        const metadata = await readJsonFromS3(key);
        const hunterId = extractHunterId(metadata, key);

        if (!isSafeId(hunterId)) continue;
        if (hunterFilter && hunterId !== hunterFilter) continue;

        const hunterProfile = {
          hunter_id: hunterId,

          nickname:
            metadata.hunter?.nickname ||
            metadata.hunter_profile?.nickname ||
            metadata.nickname ||
            metadata.public_hunter_code ||
            metadata.hunter?.public_hunter_code ||
            metadata.hunter_profile?.public_hunter_code ||
            '미등록',

          phone:
            metadata.hunter?.phone ||
            metadata.hunter?.phone_number ||
            metadata.hunter?.phoneNumber ||
            metadata.hunter_profile?.phone ||
            metadata.hunter_profile?.phone_number ||
            metadata.hunter_profile?.phoneNumber ||
            metadata.phone ||
            metadata.phone_number ||
            metadata.phoneNumber ||
            '미수집',

          country:
            metadata.hunter?.country ||
            metadata.hunter_profile?.country ||
            metadata.location?.country ||
            metadata.country ||
            '미수집',

          city:
            metadata.hunter?.city ||
            metadata.hunter_profile?.city ||
            metadata.location?.city ||
            metadata.city ||
            '미수집',

          city_code:
            metadata.hunter?.city_code ||
            metadata.hunter?.cityCode ||
            metadata.hunter_profile?.city_code ||
            metadata.hunter_profile?.cityCode ||
            metadata.location?.city_code ||
            metadata.location?.cityCode ||
            metadata.city_code ||
            metadata.cityCode ||
            '미수집',

          public_hunter_code:
            metadata.hunter?.public_hunter_code ||
            metadata.hunter?.publicHunterCode ||
            metadata.hunter_profile?.public_hunter_code ||
            metadata.hunter_profile?.publicHunterCode ||
            metadata.public_hunter_code ||
            metadata.publicHunterCode ||
            '미수집',

          recruited_by_hunter_id:
            metadata.hunter?.recruited_by_hunter_id ||
            metadata.hunter?.recruitedByHunterId ||
            metadata.hunter_profile?.recruited_by_hunter_id ||
            metadata.hunter_profile?.recruitedByHunterId ||
            metadata.recruited_by_hunter_id ||
            metadata.recruitedByHunterId ||
            metadata.referrer_hunter_id ||
            metadata.referrerHunterId ||
            metadata.referral_code ||
            metadata.hunter?.referral_code ||
            metadata.hunter_profile?.referral_code ||
            metadata.referral_code_input ||
            metadata.hunter?.referral_code_input ||
            metadata.hunter_profile?.referral_code_input ||
            null,

          leader_hunter_id:
            metadata.hunter?.leader_hunter_id ||
            metadata.hunter?.leaderHunterId ||
            metadata.hunter_profile?.leader_hunter_id ||
            metadata.hunter_profile?.leaderHunterId ||
            metadata.leader_hunter_id ||
            metadata.leaderHunterId ||
            null,
        };

        const captureDate = extractCaptureDate(metadata, key);
        const durationMinutes = getDurationMinutes(metadata);

        const review = getAutoReview(metadata, {
          uploadComplete,
          uploadMissing: uploadStatus.missing,
        });

        const estimatedEarning = calculatePayableEarning(metadata, review);

        const item = {
          s3_key: key,
          s3_prefix: prefix,

          hunter_id: hunterId,
          hunter: hunterProfile,

          capture_date: captureDate,
          sort_time_ms: extractCaptureSortTime(metadata, key),
          duration_minutes: Number(durationMinutes.toFixed(2)),

          upload_complete: uploadComplete,
          missing_files: uploadStatus.missing,

          status: review.status,
          payable: review.payable,
          reject_reasons: review.reasons,
          review_warnings: review.warnings,
          quality_bucket: review.quality_bucket,

          estimated_earning_usd: estimatedEarning,

          video_key: uploadStatus.keys.video,
          capture_metadata_key: uploadStatus.keys.captureMetadata,
          imu_metadata_key: uploadStatus.keys.imuMetadata,
        };

        if (statusFilter && item.status !== statusFilter) continue;

        items.push(item);
      } catch (err) {
        console.error('ADMIN_REVIEW_UPLOAD_ITEM_ERROR', err);
      }
    }

    items.sort((a, b) => {
      const dateCompare = String(b.capture_date || '').localeCompare(String(a.capture_date || ''));
      if (dateCompare !== 0) return dateCompare;
      return String(b.s3_key || '').localeCompare(String(a.s3_key || ''));
    });

    return res.json({
      success: true,
      total: items.length,
      hunter_filter: hunterFilter,
      status_filter: statusFilter,
      items: items.slice(0, limit),
    });
  } catch (error) {
    console.error('ADMIN_REVIEW_UPLOADS_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to load review uploads',
    });
  }
});

app.get('/admin/review-sessions', async (req, res) => {
  try {
    const hunterFilter = req.query.hunter_id || req.query.hunterId || null;
    const statusFilter = req.query.status || null;
    const limit = Math.min(Number(req.query.limit || 200), 1000);

    const keys = await listCaptureMetadataKeys();
    const items = [];

    for (const key of keys) {
      try {
        const prefix = getCapturePrefixFromMetadataKey(key);
        const uploadStatus = await getUploadFileStatus(prefix);
        const uploadComplete = uploadStatus.strictComplete;

        const metadata = await readJsonFromS3(key);
        const hunterId = extractHunterId(metadata, key);

        if (!isSafeId(hunterId)) continue;
        if (hunterFilter && hunterId !== hunterFilter) continue;

        const hunterProfile = {
          hunter_id: hunterId,

          nickname:
            metadata.hunter?.nickname ||
            metadata.hunter_profile?.nickname ||
            metadata.nickname ||
            metadata.public_hunter_code ||
            metadata.hunter?.public_hunter_code ||
            metadata.hunter_profile?.public_hunter_code ||
            '미등록',

          phone:
            metadata.hunter?.phone ||
            metadata.hunter?.phone_number ||
            metadata.hunter?.phoneNumber ||
            metadata.hunter_profile?.phone ||
            metadata.hunter_profile?.phone_number ||
            metadata.hunter_profile?.phoneNumber ||
            metadata.phone ||
            metadata.phone_number ||
            metadata.phoneNumber ||
            '미수집',

          country:
            metadata.hunter?.country ||
            metadata.hunter_profile?.country ||
            metadata.location?.country ||
            metadata.country ||
            '미수집',

          city:
            metadata.hunter?.city ||
            metadata.hunter_profile?.city ||
            metadata.location?.city ||
            metadata.city ||
            '미수집',

          recruited_by_hunter_id:
            metadata.hunter?.recruited_by_hunter_id ||
            metadata.hunter?.recruitedByHunterId ||
            metadata.hunter_profile?.recruited_by_hunter_id ||
            metadata.hunter_profile?.recruitedByHunterId ||
            metadata.recruited_by_hunter_id ||
            metadata.recruitedByHunterId ||
            metadata.referrer_hunter_id ||
            metadata.referrerHunterId ||
            metadata.referral_code ||
            metadata.hunter?.referral_code ||
            metadata.hunter_profile?.referral_code ||
            metadata.referral_code_input ||
            metadata.hunter?.referral_code_input ||
            metadata.hunter_profile?.referral_code_input ||
            null,
        };

        const captureDate = extractCaptureDate(metadata, key);
        const durationMinutes = getDurationMinutes(metadata);

        const review = getAutoReview(metadata, {
          uploadComplete,
          uploadMissing: uploadStatus.missing,
        });

        const estimatedEarning = calculatePayableEarning(metadata, review);

        items.push({
          s3_key: key,
          s3_prefix: prefix,

          hunter_id: hunterId,
          hunter: hunterProfile,

          capture_date: captureDate,
          sort_time_ms: extractCaptureSortTime(metadata, key),
          duration_minutes: Number(durationMinutes.toFixed(2)),

          upload_complete: uploadComplete,
          missing_files: uploadStatus.missing,

          status: review.status,
          payable: review.payable,
          reject_reasons: review.reasons,
          review_warnings: review.warnings,
          quality_bucket: review.quality_bucket,

          estimated_earning_usd: estimatedEarning,

          video_key: uploadStatus.keys.video,
          capture_metadata_key: uploadStatus.keys.captureMetadata,
          imu_metadata_key: uploadStatus.keys.imuMetadata,
        });
      } catch (err) {
        console.error('ADMIN_REVIEW_SESSION_ITEM_ERROR', err);
      }
    }

    let sessions = groupReviewItemsIntoSessions(items, 5);

    if (statusFilter) {
      sessions = sessions.filter((session) => session.status === statusFilter);
    }

    return res.json({
      success: true,
      total: sessions.length,
      hunter_filter: hunterFilter,
      status_filter: statusFilter,
      grouping: {
        mode: 'server_time_gap',
        gap_minutes: 5,
        note: 'Raw files are not moved or merged. Sessions are logical groups only.',
      },
      items: sessions.slice(0, limit),
    });
  } catch (error) {
    console.error('ADMIN_REVIEW_SESSIONS_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to load review sessions',
    });
  }
});

app.get('/admin/video-url', async (req, res) => {
  try {
    const key = req.query.key;

    if (!key || typeof key !== 'string') {
      return res.status(400).json({
        success: false,
        message: 'key is required',
      });
    }

    // S3 GetObject presigned URL 생성
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });

    const url = await getSignedUrl(s3, command, {
      expiresIn: 300, // 5분
    });

    return res.json({
      success: true,
      key,
      url,
      expires_in_seconds: 300,
    });

  } catch (error) {
    console.error('VIDEO_URL_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to generate video URL',
    });
  }
});

app.get('/admin/incomplete-uploads', async (req, res) => {
  try {
    const hunterFilter = req.query.hunter_id || req.query.hunterId || null;
    const limit = Math.min(Number(req.query.limit || 200), 1000);
    const objects = await listKeysByPrefix('real/v1/raw/');
    const prefixMap = {};

    for (const item of objects) {
      const match = item.key.match(/^(real\/v1\/raw\/\d{4}-\d{2}-\d{2}\/([A-Za-z0-9_-]{3,120})\/([A-Za-z0-9_-]{3,120})\/)/);
      if (!match) continue;

      const prefix = match[1];
      const hunterId = match[2];
      const captureId = match[3];
      const fileName = item.key.slice(prefix.length);

      if (hunterFilter && hunterId !== hunterFilter) continue;

      if (!prefixMap[prefix]) {
        prefixMap[prefix] = {
          prefix,
          hunter_id: hunterId,
          capture_id: captureId,
          files: [],
          file_names: [],
          total_size: 0,
          last_modified: null,
        };
      }

      prefixMap[prefix].files.push(item);
      prefixMap[prefix].file_names.push(fileName);
      prefixMap[prefix].total_size += item.size;
      if (!prefixMap[prefix].last_modified || item.lastModified > prefixMap[prefix].last_modified) {
        prefixMap[prefix].last_modified = item.lastModified;
      }
    }

    const incomplete = [];

    for (const capture of Object.values(prefixMap)) {
      const missing = [];
      if (!capture.file_names.includes(REQUIRED_UPLOAD_FILES.video)) missing.push(REQUIRED_UPLOAD_FILES.video);
      if (!capture.file_names.includes(REQUIRED_UPLOAD_FILES.captureMetadata)) missing.push(REQUIRED_UPLOAD_FILES.captureMetadata);
      if (!capture.file_names.includes(REQUIRED_UPLOAD_FILES.imuMetadata)) missing.push(REQUIRED_UPLOAD_FILES.imuMetadata);

      const hasClientOrServerComplete = capture.file_names.includes(UPLOAD_COMPLETE_FILE);
      const strictComplete = missing.length === 0 && hasClientOrServerComplete;

      if (!strictComplete) {
        incomplete.push({
          ...capture,
          missing,
          has_video_only:
            capture.file_names.includes(REQUIRED_UPLOAD_FILES.video) &&
            !capture.file_names.includes(REQUIRED_UPLOAD_FILES.captureMetadata) &&
            !capture.file_names.includes(REQUIRED_UPLOAD_FILES.imuMetadata),
          server_marked_complete: hasClientOrServerComplete,
          status: missing.length > 0 ? 'INCOMPLETE_UPLOAD' : 'WAITING_SERVER_COMPLETE_MARKER',
        });
      }
    }

    incomplete.sort((a, b) => String(b.last_modified).localeCompare(String(a.last_modified)));

    return res.json({
      success: true,
      bucket: BUCKET,
      total_incomplete: incomplete.length,
      hunter_filter: hunterFilter,
      items: incomplete.slice(0, limit),
    });
  } catch (error) {
    console.error('ADMIN_INCOMPLETE_UPLOADS_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to scan incomplete uploads',
    });
  }
});

app.get('/hunter/earnings', async (req, res) => {
  try {
    const hunterId = req.query.hunter_id || req.query.hunterId;

    if (!isSafeId(hunterId)) {
      return res.status(400).json({
        success: false,
        message: 'Valid hunter_id is required',
      });
    }

    const today = todayKst();
    const keys = await listCaptureMetadataKeys();

    let todayEarnings = 0;
    let total = 0;
    let matchedUploads = 0;
    let rejectedUploads = 0;
    let holdUploads = 0;
    let payableUploads = 0;
    let skippedIncompleteUploads = 0;

    const items = [];

    for (const key of keys) {
      try {
        const prefix = getCapturePrefixFromMetadataKey(key);
        const uploadStatus = await getUploadFileStatus(prefix);
        const uploadComplete = uploadStatus.strictComplete;

        if (!uploadComplete) {
          skippedIncompleteUploads += 1;
        }

        const metadata = await readJsonFromS3(key);
        const metadataHunterId = extractHunterId(metadata, key);

        if (metadataHunterId !== hunterId) continue;

        const captureDate = extractCaptureDate(metadata, key);
        const durationMinutes = getDurationMinutes(metadata);

        const review = getAutoReview(metadata, {
          uploadComplete,
          uploadMissing: uploadStatus.missing,
        });

        const estimated = calculatePayableEarning(metadata, review);

        matchedUploads += 1;

        if (review.status === 'REJECT') {
          rejectedUploads += 1;
        } else if (review.status === 'HOLD') {
          holdUploads += 1;
        } else if (review.payable) {
          payableUploads += 1;
          total += estimated;

          if (captureDate === today) {
            todayEarnings += estimated;
          }
        }

        items.push({
          s3_key: key,
          capture_date: captureDate,
          duration_minutes: Number(durationMinutes.toFixed(2)),
          estimated_earning: estimated,
          status: review.status,
          payable: review.payable,
          reject_reasons: review.reasons,
        });

      } catch (err) {
        console.error('EARNINGS_ITEM_ERROR', err);
      }
    }

    return res.json({
      success: true,
      hunter_id: hunterId,
      today_earnings: Number(todayEarnings.toFixed(2)),
      total: Number(total.toFixed(2)),
      payable_uploads: payableUploads,
      rejected_uploads: rejectedUploads,
      hold_uploads: holdUploads,
      matched_uploads: matchedUploads,
      skipped_incomplete_uploads: skippedIncompleteUploads,
      items,
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ success: false });
  }
});

app.get('/hunter/dashboard', async (req, res) => {
  try {
    const hunterId = req.query.hunter_id || req.query.hunterId;

    if (!isSafeId(hunterId)) {
      return res.status(400).json({
        success: false,
        message: 'Valid hunter_id is required',
      });
    }

    const today = todayKst();
    const keys = await listCaptureMetadataKeys();

    let todayUploads = 0;
    let totalUploads = 0;
    let todayEarnings = 0;
    let totalEarnings = 0;
    let approvedUploads = 0;
    let holdUploads = 0;
    let rejectedUploads = 0;

    for (const key of keys) {
      try {
        const metadata = await readJsonFromS3(key);
        const metadataHunterId = extractHunterId(metadata, key);

        if (metadataHunterId !== hunterId) continue;

        const captureDate = extractCaptureDate(metadata, key);

        const review = getAutoReview(metadata, {
          uploadComplete: true,
          uploadMissing: [],
        });

        const estimated = calculatePayableEarning(metadata, review);

        totalUploads += 1;

        if (review.status === 'REJECT') {
          rejectedUploads += 1;
        } else if (review.status === 'HOLD') {
          holdUploads += 1;
        } else if (review.payable) {
          approvedUploads += 1;
          totalEarnings += estimated;
        }

        if (captureDate === today) {
          todayUploads += 1;

          if (review.payable) {
            todayEarnings += estimated;
          }
        }

      } catch (err) {
        console.error('DASHBOARD_ITEM_ERROR', err);
      }
    }

    return res.json({
      success: true,
      hunter_id: hunterId,
      currency: 'USD',
      earnings: {
        today: Number(todayEarnings.toFixed(2)),
        total: Number(totalEarnings.toFixed(2)),
      },
      stats: {
        uploaded: totalUploads,
        approved: approvedUploads,
        hold: holdUploads,
        rejected: rejectedUploads,
      },
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ success: false });
  }
});

app.get('/hunter/rankings', async (req, res) => {
  try {
    const currentHunterId = req.query.hunter_id || req.query.hunterId || null;

    const keys = await listCaptureMetadataKeys();
    const hunterMap = {};

    for (const key of keys) {
      try {
        const metadata = await readJsonFromS3(key);
        const hunterId = extractHunterId(metadata, key);

        if (!isSafeId(hunterId)) continue;

        const review = getAutoReview(metadata, {
          uploadComplete: true,
          uploadMissing: [],
        });

        const estimated = calculatePayableEarning(metadata, review);
        const durationMinutes = getDurationMinutes(metadata);

        if (!hunterMap[hunterId]) {
          hunterMap[hunterId] = {
            hunter_id: hunterId,
            total_uploads: 0,
            payable_uploads: 0,
            hold_uploads: 0,
            rejected_uploads: 0,
            total_earnings: 0,
            total_minutes: 0,
          };
        }

        hunterMap[hunterId].total_uploads += 1;

        if (review.status === 'REJECT') {
          hunterMap[hunterId].rejected_uploads += 1;
        } else if (review.status === 'HOLD') {
          hunterMap[hunterId].hold_uploads += 1;
        } else if (review.payable) {
          hunterMap[hunterId].payable_uploads += 1;
          hunterMap[hunterId].total_earnings += estimated;
          hunterMap[hunterId].total_minutes += durationMinutes;
        }

      } catch (err) {
        console.error('RANKINGS_ITEM_ERROR', err);
      }
    }

    const rankings = Object.values(hunterMap)
      .sort((a, b) => b.total_earnings - a.total_earnings)
      .map((item, index) => ({
        rank: index + 1,
        hunter_id: item.hunter_id,
        total_uploads: item.total_uploads,
        payable_uploads: item.payable_uploads,
        hold_uploads: item.hold_uploads,
        rejected_uploads: item.rejected_uploads,
        total_earnings: Number(item.total_earnings.toFixed(2)),
        total_minutes: Number(item.total_minutes.toFixed(2)),
        isCurrentHunter: currentHunterId === item.hunter_id,
      }));

    return res.json({
      success: true,
      rankings,
      totalHunters: rankings.length,
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ success: false });
  }
});

app.get('/hunter/payout-summary', async (req, res) => {
  try {
    const hunterId = req.query.hunter_id;

    if (!hunterId) {
      return res.status(400).json({
        success: false,
        message: 'hunter_id is required',
      });
    }

    const keys = await listCaptureMetadataKeys();

    let captureEarnings = 0;
    let matchedUploads = 0;
    let payableUploads = 0;
    let rejectedUploads = 0;
    let holdUploads = 0;

    for (const key of keys) {
      try {
        const prefix = getCapturePrefixFromMetadataKey(key);
        const uploadStatus = await getUploadFileStatus(prefix);

        if (!uploadStatus.strictComplete) continue;

        const metadata = await readJsonFromS3(key);
        const metadataHunterId = extractHunterId(metadata, key);

        if (metadataHunterId !== hunterId) continue;

        const review = getAutoReview(metadata, {
          uploadComplete: uploadStatus.strictComplete,
          uploadMissing: uploadStatus.missing,
        });

        const estimated = calculatePayableEarning(metadata, review);

        matchedUploads += 1;

        if (review.status === 'REJECT') {
          rejectedUploads += 1;
        } else if (review.status === 'HOLD') {
          holdUploads += 1;
        } else if (review.payable) {
          payableUploads += 1;
          captureEarnings += estimated;
        }
      } catch (itemError) {
        console.error('PAYOUT_SUMMARY_ITEM_ERROR:', itemError);
      }
    }

    const payoutRequests = readPayoutRequests()
      .filter((item) => item.hunter_id === hunterId);

    const paidTotal = payoutRequests
      .filter((item) => item.status === 'paid')
      .reduce((sum, item) => sum + Number(item.amount || 0), 0);

    const pendingPayout = payoutRequests
      .filter((item) => item.status === 'requested' || item.status === 'approved')
      .reduce((sum, item) => sum + Number(item.amount || 0), 0);

    const recruitEarnings = 0;
    const leaderBonus = 0;

    const totalEarnings = captureEarnings + recruitEarnings + leaderBonus;
    const availableBalance = Math.max(totalEarnings - paidTotal - pendingPayout, 0);

    return res.json({
      success: true,
      hunter_id: hunterId,
      currency: 'USD',

      capture_earnings: Number(captureEarnings.toFixed(2)),
      recruit_earnings: Number(recruitEarnings.toFixed(2)),
      leader_bonus: Number(leaderBonus.toFixed(2)),
      total_earnings: Number(totalEarnings.toFixed(2)),

      paid_total: Number(paidTotal.toFixed(2)),
      pending_payout: Number(pendingPayout.toFixed(2)),
      available_balance: Number(availableBalance.toFixed(2)),

      minimum_payout: 8,

      matched_uploads: matchedUploads,
      payable_uploads: payableUploads,
      rejected_uploads: rejectedUploads,
      hold_uploads: holdUploads,

      payout_requests: payoutRequests,
    });
  } catch (error) {
    console.error('PAYOUT_SUMMARY_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to load payout summary',
    });
  }
});

app.post('/hunter/payout-request', (req, res) => {
  try {
    const { hunter_id, amount } = req.body;

    if (!hunter_id || !amount) {
      return res.status(400).json({
        success: false,
        message: 'hunter_id and amount are required',
      });
    }

    const items = readPayoutRequests();

    const newRequest = {
      request_id: `PR-${Date.now()}`,
      hunter_id,
      amount: Number(amount),
      status: 'requested',
      created_at: new Date().toISOString(),
    };

    items.push(newRequest);
    writePayoutRequests(items);

    return res.json({
      success: true,
      request: newRequest,
    });
  } catch (error) {
    console.error('PAYOUT_REQUEST_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to create payout request',
    });
  }
});

app.get('/admin/payout-requests', (req, res) => {
  try {
    const items = readPayoutRequests();

    const sortedItems = items.sort((a, b) => {
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });

    return res.json({
      success: true,
      total: sortedItems.length,
      items: sortedItems,
    });
  } catch (error) {
    console.error('ADMIN_PAYOUT_REQUESTS_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to load payout requests',
    });
  }
});

app.get('/admin/payout-summary', async (req, res) => {
  try {
    const keys = await listCaptureMetadataKeys();
    const payoutRequests = readPayoutRequests();

    const hunterMap = {};

    for (const key of keys) {
      try {
        const prefix = getCapturePrefixFromMetadataKey(key);
        const uploadStatus = await getUploadFileStatus(prefix);

        if (!uploadStatus.strictComplete) continue;

        const metadata = await readJsonFromS3(key);
        const hunterId = extractHunterId(metadata, key);

        if (!hunterId) continue;

        const nickname =
          metadata.hunter?.nickname ||
          metadata.hunter_profile?.nickname ||
          metadata.nickname ||
          '';

        const phone =
          metadata.hunter?.phone ||
          metadata.hunter?.phone_number ||
          metadata.hunter?.phoneNumber ||
          metadata.hunter_profile?.phone ||
          metadata.hunter_profile?.phone_number ||
          metadata.hunter_profile?.phoneNumber ||
          metadata.phone ||
          metadata.phone_number ||
          metadata.phoneNumber ||
          '';

        const country =
          metadata.hunter?.country ||
          metadata.hunter_profile?.country ||
          metadata.location?.country ||
          metadata.country ||
          '';

        const city =
          metadata.hunter?.city ||
          metadata.hunter_profile?.city ||
          metadata.location?.city ||
          metadata.city ||
          '';

        const recruitedByHunterId =
          metadata.hunter?.recruited_by_hunter_id ||
          metadata.hunter?.recruitedByHunterId ||
          metadata.hunter_profile?.recruited_by_hunter_id ||
          metadata.hunter_profile?.recruitedByHunterId ||
          metadata.recruited_by_hunter_id ||
          metadata.recruitedByHunterId ||
          metadata.referrer_hunter_id ||
          metadata.referrerHunterId ||
          metadata.referral_code ||
          metadata.hunter?.referral_code ||
          metadata.hunter_profile?.referral_code ||
          metadata.referral_code_input ||
          metadata.hunter?.referral_code_input ||
          metadata.hunter_profile?.referral_code_input ||
          '';

        if (!hunterMap[hunterId]) {
          hunterMap[hunterId] = {
            hunter_id: hunterId,
            nickname,
            phone,
            country,
            city,
            recruited_by_hunter_id: recruitedByHunterId,
            latest_sort_key: key,
            capture_earnings: 0,
            recruit_earnings: 0,
            leader_bonus: 0,
            total_earnings: 0,
            paid_total: 0,
            pending_payout: 0,
            available_balance: 0,
            total_uploads: 0,
            total_minutes: 0,
            payable_minutes: 0,
            payable_uploads: 0,
            rejected_uploads: 0,
            hold_uploads: 0,
          };
        } else {
          if (!hunterMap[hunterId].phone && phone) {
            hunterMap[hunterId].phone = phone;
          }

          if (!hunterMap[hunterId].nickname && nickname) {
            hunterMap[hunterId].nickname = nickname;
          }

          if (!hunterMap[hunterId].country && country) {
            hunterMap[hunterId].country = country;
          }

          if (!hunterMap[hunterId].city && city) {
            hunterMap[hunterId].city = city;
          }

          if (!hunterMap[hunterId].recruited_by_hunter_id && recruitedByHunterId) {
            hunterMap[hunterId].recruited_by_hunter_id = recruitedByHunterId;
          }

          if (String(key) > String(hunterMap[hunterId].latest_sort_key || '')) {
            hunterMap[hunterId].latest_sort_key = key;
          }
        }

        const review = getAutoReview(metadata, {
          uploadComplete: uploadStatus.strictComplete,
          uploadMissing: uploadStatus.missing,
        });

        const estimated = calculatePayableEarning(metadata, review);
        const durationMinutes = getDurationMinutes(metadata);

        hunterMap[hunterId].total_uploads += 1;
        hunterMap[hunterId].total_minutes += durationMinutes;

        if (review.status === 'REJECT') {
          hunterMap[hunterId].rejected_uploads += 1;
        } else if (review.status === 'HOLD') {
          hunterMap[hunterId].hold_uploads += 1;
        } else if (review.payable) {
          hunterMap[hunterId].payable_uploads += 1;
          hunterMap[hunterId].payable_minutes += durationMinutes;
          hunterMap[hunterId].capture_earnings += estimated;
        }
      } catch (itemError) {
        console.error('ADMIN_PAYOUT_SUMMARY_ITEM_ERROR:', itemError);
      }
    }

    for (const item of payoutRequests) {
      if (!hunterMap[item.hunter_id]) {
        hunterMap[item.hunter_id] = {
          hunter_id: item.hunter_id,
          nickname: '',
          phone: '',
          country: '',
          city: '',
          recruited_by_hunter_id: '',
          latest_sort_key: item.created_at || '',
          capture_earnings: 0,
          recruit_earnings: 0,
          leader_bonus: 0,
          total_earnings: 0,
          paid_total: 0,
          pending_payout: 0,
          available_balance: 0,
          total_uploads: 0,
          total_minutes: 0,
          payable_minutes: 0,
          payable_uploads: 0,
          rejected_uploads: 0,
          hold_uploads: 0,
        };
      }

      if (item.created_at && String(item.created_at) > String(hunterMap[item.hunter_id].latest_sort_key || '')) {
        hunterMap[item.hunter_id].latest_sort_key = item.created_at;
      }

      if (item.status === 'paid') {
        hunterMap[item.hunter_id].paid_total += Number(item.amount || 0);
      }

      if (item.status === 'requested' || item.status === 'approved') {
        hunterMap[item.hunter_id].pending_payout += Number(item.amount || 0);
      }
    }

    const hunters = Object.values(hunterMap).map((hunter) => {
      hunter.total_earnings =
        hunter.capture_earnings + hunter.recruit_earnings + hunter.leader_bonus;

      hunter.available_balance = Math.max(
        hunter.total_earnings - hunter.paid_total - hunter.pending_payout,
        0
      );

      return {
        ...hunter,
        capture_earnings: Number(hunter.capture_earnings.toFixed(2)),
        recruit_earnings: Number(hunter.recruit_earnings.toFixed(2)),
        leader_bonus: Number(hunter.leader_bonus.toFixed(2)),
        total_earnings: Number(hunter.total_earnings.toFixed(2)),
        paid_total: Number(hunter.paid_total.toFixed(2)),
        pending_payout: Number(hunter.pending_payout.toFixed(2)),
        available_balance: Number(hunter.available_balance.toFixed(2)),
        total_minutes: Number((hunter.total_minutes || 0).toFixed(2)),
        payable_minutes: Number((hunter.payable_minutes || 0).toFixed(2)),
      };
    }).sort((a, b) => {
      return String(b.latest_sort_key || '').localeCompare(String(a.latest_sort_key || ''));
    });

    return res.json({
      success: true,
      total_hunters: hunters.length,
      hunters,
      payout_requests: payoutRequests,
    });
  } catch (error) {
    console.error('ADMIN_PAYOUT_SUMMARY_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to load admin payout summary',
    });
  }
});

app.post('/admin/payout-requests/:request_id/approve', (req, res) => {
  try {
    const { request_id } = req.params;

    const items = readPayoutRequests();
    const target = items.find((item) => item.request_id === request_id);

    if (!target) {
      return res.status(404).json({
        success: false,
        message: 'Payout request not found',
      });
    }

    if (target.status !== 'requested') {
      return res.status(400).json({
        success: false,
        message: `Only requested payout can be approved. Current status: ${target.status}`,
      });
    }

    target.status = 'approved';
    target.approved_at = new Date().toISOString();

    writePayoutRequests(items);

    return res.json({
      success: true,
      request: target,
    });
  } catch (error) {
    console.error('ADMIN_PAYOUT_APPROVE_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to approve payout request',
    });
  }
});

app.post('/admin/payout-requests/:request_id/paid', (req, res) => {
  try {
    const { request_id } = req.params;

    const items = readPayoutRequests();
    const target = items.find((item) => item.request_id === request_id);

    if (!target) {
      return res.status(404).json({
        success: false,
        message: 'Payout request not found',
      });
    }

    if (target.status !== 'approved') {
      return res.status(400).json({
        success: false,
        message: `Only approved payout can be marked as paid. Current status: ${target.status}`,
      });
    }

    target.status = 'paid';
    target.paid_at = new Date().toISOString();

    writePayoutRequests(items);

    return res.json({
      success: true,
      request: target,
    });
  } catch (error) {
    console.error('ADMIN_PAYOUT_PAID_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to mark payout as paid',
    });
  }
});

app.post('/api/v1/s3-presign', async (req, res) => {
  try {
    const { prefix, files } = req.body || {};

    if (!prefix || !files?.video || !files?.captureMetadata || !files?.imuMetadata) {
      return res.status(400).json({
        success: false,
        message: 'prefix and files.video/captureMetadata/imuMetadata are required',
      });
    }

    const safePrefix = prefix.startsWith('real/') ? prefix : `real/${prefix}`;

    const videoKey = `${safePrefix}${files.video}`;
    const captureMetadataKey = `${safePrefix}${files.captureMetadata}`;
    const imuMetadataKey = `${safePrefix}${files.imuMetadata}`;

    const videoCommand = new PutObjectCommand({
      Bucket: BUCKET,
      Key: videoKey,
      ContentType: 'video/mp4',
    });

    const captureMetadataCommand = new PutObjectCommand({
      Bucket: BUCKET,
      Key: captureMetadataKey,
      ContentType: 'application/json',
    });

    const imuMetadataCommand = new PutObjectCommand({
      Bucket: BUCKET,
      Key: imuMetadataKey,
      ContentType: 'application/json',
    });

    const videoUrl = await getSignedUrl(s3, videoCommand, {
      expiresIn: 3600,
    });

    const captureMetadataUrl = await getSignedUrl(
      s3,
      captureMetadataCommand,
      {
        expiresIn: 3600,
      }
    );

    const imuMetadataUrl = await getSignedUrl(
      s3,
      imuMetadataCommand,
      {
        expiresIn: 3600,
      }
    );

    console.log('S3_PRESIGN_CREATED', {
      prefix,
    });

    return res.json({
      success: true,
      s3Prefix: prefix,
      uploadCompleteMode: 'server_verified_only',
      finalizeEndpoint: '/api/v1/upload-complete',
      urls: {
        video: videoUrl,
        captureMetadata: captureMetadataUrl,
        imuMetadata: imuMetadataUrl,
      },
    });
  } catch (error) {
    console.error('S3_PRESIGN_ERROR:', error);

    return res.status(500).json({
      success: false,
      message: 'Failed to create presigned URLs',
    });
  }
});

app.post('/api/v1/s3-multipart/create', async (req, res) => {
  try {
    const parsed = normalizeMultipartVideoKey(req.body);

    if (!parsed) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_MULTIPART_KEY',
        message: 'Multipart upload is allowed only for real/v1/raw/.../video_raw.mp4',
      });
    }

    const contentType = req.body?.contentType || 'video/mp4';

    if (contentType !== 'video/mp4') {
      return res.status(400).json({
        success: false,
        error: 'INVALID_CONTENT_TYPE',
        message: 'Only video/mp4 is allowed for multipart video upload',
      });
    }

    const command = new CreateMultipartUploadCommand({
      Bucket: BUCKET,
      Key: parsed.key,
      ContentType: 'video/mp4',
      Metadata: {
        upload_transport_mode: 's3_multipart',
        dataset_segment_mode: 'server_only_after_raw_complete',
        hunter_id: parsed.hunterId,
        capture_id: parsed.captureId,
      },
    });

    const result = await s3.send(command);

    console.log('S3_MULTIPART_CREATE_DONE', {
      key: parsed.key,
      uploadId: result.UploadId,
      hunterId: parsed.hunterId,
      captureId: parsed.captureId,
      datasetSegmentMode: 'server_only_after_raw_complete',
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      key: parsed.key,
      s3Prefix: parsed.prefix,
      uploadId: result.UploadId,
      contentType: 'video/mp4',
      uploadTransportMode: 's3_multipart',
      datasetSegmentMode: 'server_only_after_raw_complete',
      message: 'Multipart upload created for raw video only',
    });
  } catch (error) {
    console.error('S3_MULTIPART_CREATE_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to create multipart upload',
    });
  }
});
app.post('/api/v1/s3-multipart/part-url', async (req, res) => {
  try {
    const parsed = normalizeMultipartVideoKey(req.body);
    const uploadId = req.body?.uploadId;
    const partNumber = Number(req.body?.partNumber);

    if (!parsed) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_MULTIPART_KEY',
        message: 'Multipart part URL is allowed only for real/v1/raw/.../video_raw.mp4',
      });
    }

    if (!uploadId || typeof uploadId !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'INVALID_UPLOAD_ID',
        message: 'Valid uploadId is required',
      });
    }

    if (!Number.isInteger(partNumber) || partNumber < 1 || partNumber > 10000) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_PART_NUMBER',
        message: 'partNumber must be an integer between 1 and 10000',
      });
    }

    const command = new UploadPartCommand({
      Bucket: BUCKET,
      Key: parsed.key,
      UploadId: uploadId,
      PartNumber: partNumber,
    });

    const url = await getSignedUrl(s3, command, {
      expiresIn: MULTIPART_EXPIRES_IN_SECONDS,
    });

    console.log('S3_MULTIPART_PART_URL_CREATED', {
      key: parsed.key,
      uploadId,
      partNumber,
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      key: parsed.key,
      uploadId,
      partNumber,
      expiresIn: MULTIPART_EXPIRES_IN_SECONDS,
      url,
    });
  } catch (error) {
    console.error('S3_MULTIPART_PART_URL_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to create multipart part URL',
    });
  }
});

app.post('/api/v1/s3-multipart/complete', async (req, res) => {
  try {
    const parsed = normalizeMultipartVideoKey(req.body);
    const uploadId = req.body?.uploadId;
    const incomingParts = Array.isArray(req.body?.parts) ? req.body.parts : [];

    if (!parsed) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_MULTIPART_KEY',
        message: 'Multipart complete is allowed only for real/v1/raw/.../video_raw.mp4',
      });
    }

    if (!uploadId || typeof uploadId !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'INVALID_UPLOAD_ID',
        message: 'Valid uploadId is required',
      });
    }

    if (incomingParts.length < 1 || incomingParts.length > 10000) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_PARTS',
        message: 'parts must contain between 1 and 10000 uploaded parts',
      });
    }

    const parts = incomingParts
      .map((part) => {
        const partNumber = Number(part.PartNumber || part.partNumber);
        const eTag = normalizeEtagForComplete(part.ETag || part.eTag || part.etag);

        return {
          PartNumber: partNumber,
          ETag: eTag,
        };
      })
      .filter((part) => {
        return Number.isInteger(part.PartNumber) &&
          part.PartNumber >= 1 &&
          part.PartNumber <= 10000 &&
          Boolean(part.ETag);
      })
      .sort((a, b) => a.PartNumber - b.PartNumber);

    if (parts.length !== incomingParts.length) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_PARTS',
        message: 'Every part must include valid PartNumber and ETag',
      });
    }

    const uniquePartNumbers = new Set(parts.map((part) => part.PartNumber));
    if (uniquePartNumbers.size !== parts.length) {
      return res.status(400).json({
        success: false,
        error: 'DUPLICATE_PART_NUMBER',
        message: 'Duplicate PartNumber found',
      });
    }

    const command = new CompleteMultipartUploadCommand({
      Bucket: BUCKET,
      Key: parsed.key,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: parts,
      },
    });

    const result = await s3.send(command);

    console.log('S3_MULTIPART_COMPLETE_DONE', {
      key: parsed.key,
      uploadId,
      partCount: parts.length,
      location: result.Location,
      datasetSegmentMode: 'server_only_after_raw_complete',
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      key: parsed.key,
      s3Prefix: parsed.prefix,
      uploadId,
      partCount: parts.length,
      eTag: result.ETag || null,
      location: result.Location || null,
      uploadTransportComplete: true,
      datasetSegmentationComplete: false,
      datasetSegmentMode: 'server_only_after_raw_complete',
      message: 'Raw video multipart upload completed. Dataset segmentation must run on server after upload-complete verification.',
    });
  } catch (error) {
    console.error('S3_MULTIPART_COMPLETE_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to complete multipart upload',
    });
  }
});

app.post('/api/v1/s3-multipart/abort', async (req, res) => {
  try {
    const parsed = normalizeMultipartVideoKey(req.body);
    const uploadId = req.body?.uploadId;

    if (!parsed) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_MULTIPART_KEY',
        message: 'Multipart abort is allowed only for real/v1/raw/.../video_raw.mp4',
      });
    }

    if (!uploadId || typeof uploadId !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'INVALID_UPLOAD_ID',
        message: 'Valid uploadId is required',
      });
    }

    const command = new AbortMultipartUploadCommand({
      Bucket: BUCKET,
      Key: parsed.key,
      UploadId: uploadId,
    });

    await s3.send(command);

    console.log('S3_MULTIPART_ABORT_DONE', {
      key: parsed.key,
      uploadId,
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      key: parsed.key,
      s3Prefix: parsed.prefix,
      uploadId,
      message: 'Multipart upload aborted',
    });
  } catch (error) {
    console.error('S3_MULTIPART_ABORT_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to abort multipart upload',
    });
  }
});

app.post('/api/v1/upload-complete', async (req, res) => {
  try {
    const incomingPrefix = req.body?.s3Prefix || req.body?.prefix;
    const parsed = normalizePrefix(incomingPrefix);

    if (!parsed) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_PREFIX',
        message: 'Valid s3Prefix is required',
      });
    }

    const { prefix, hunterId, captureId } = parsed;
    const uploadStatus = await getUploadFileStatus(prefix);

    if (!uploadStatus.requiredComplete) {
      return res.status(400).json({
        success: false,
        error: 'INCOMPLETE_UPLOAD',
        message: 'video_raw.mp4, capture_metadata_v1.json, and imu_metadata_v1.json are all required before completion',
        s3Prefix: prefix,
        missing: uploadStatus.missing,
        fileStatus: uploadStatus.files,
      });
    }

    const metadata = await readJsonFromS3(uploadStatus.keys.captureMetadata);
    const metadataHunterId = extractHunterId(metadata, uploadStatus.keys.captureMetadata);

    if (metadataHunterId && metadataHunterId !== hunterId) {
      return res.status(400).json({
        success: false,
        error: 'HUNTER_ID_MISMATCH',
        message: 'hunter_id in S3 prefix does not match hunter_id in capture metadata',
        prefixHunterId: hunterId,
        metadataHunterId,
      });
    }

    const durationMinutes = getDurationMinutes(metadata);
    const durationMs = durationMinutes * 60 * 1000;

    if (!durationMs || durationMs < MIN_DURATION_MS) {
      return res.status(400).json({
        success: false,
        error: 'UNDER_MIN_DURATION',
        message: 'Video duration must be at least 30 seconds before completion',
        duration_minutes: Number(durationMinutes.toFixed(2)),
        minimum_duration_minutes: 0.5,
      });
    }

    const review = getAutoReview(metadata, {
      uploadComplete: true,
      uploadMissing: [],
    });

    const marker = await createServerUploadCompleteMarker({
      prefix,
      hunterId,
      captureId,
      uploadStatus,
      metadata,
      review,
    });

    return res.json({
      success: true,
      message: 'Upload verified and completed by server',
      s3Prefix: prefix,
      hunter_id: hunterId,
      capture_id: captureId,
      duration_minutes: Number(durationMinutes.toFixed(2)),
      status: review.status,
      payable: review.payable,
      reject_reasons: review.reasons,
      review_warnings: review.warnings,
      uploadTransportComplete: true,
      datasetSegmentationRequested: true,
      datasetSegmentMode: 'server_only_after_raw_complete',
      marker,
    });
  } catch (error) {
    console.error('UPLOAD_COMPLETE_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to verify upload completion',
    });
  }
});

app.get('/api/v1/today-mission', (req, res) => {
  const hunterId = req.query.hunter_id || 'UNKNOWN';

  // 일단 고정 → 나중에 자동화
  const mission = {
    id: "mission_traffic_basic",
    title: "Street Traffic Capture",
    description: "Record moving traffic for at least 90 seconds.",
    minimum_minutes: 1.5,
    recommended_minutes: 10,
    target_scene: "road_traffic",
    camera_mode: "forward",
    reward_hint: "Higher quality = higher earning",
  };

  return res.json({
    success: true,
    hunter_id: hunterId,
    mission,
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`S3 Presign Server running on http://0.0.0.0:${PORT}`);
});