require('dotenv').config();

const express = require('express');
const cors = require('cors');
const {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

app.use(express.json({ limit: '2mb' }));

const PORT = process.env.PORT || 5050;
const REGION = process.env.AWS_REGION;
const BUCKET = process.env.S3_BUCKET;

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
const MIN_DURATION_MS = 180000;

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

const hasUploadComplete = async (prefix) => {
  const status = await getUploadFileStatus(prefix);
  return status.strictComplete === true;
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
    reasons.push('GPS_REJECT_RECOMMENDED');
  }

  if (segmentabilityHint.reject_segment_candidate === true) {
    reasons.push('SEGMENT_REJECT_RECOMMENDED');
  }

  if (segmentSaleability.reject_segment_candidate === true) {
    reasons.push('SEGMENT_SALEABILITY_REJECT');
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
    reasons.push('DARK_LOW_SALE_PROBABILITY');
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
    reasons.push('LOW_QUALITY_SCORE');
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
      status: 'HOLD',
      payable: false,
      review_required: true,
      reasons: [],
      warnings: uniqueWarnings,
      quality_bucket: 'hold',
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

app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 's3-presign-server',
    bucket: BUCKET,
    region: REGION,
    upload_complete_mode: 'server_verified_only',
    required_files: REQUIRED_UPLOAD_FILES,
    minimum_duration_minutes: 3,
  });
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

        if (review.status === 'REJECT') rejectedUploads += 1;
        if (review.status === 'HOLD') holdUploads += 1;
        if (review.payable) payableUploads += 1;

        total += estimated;

        if (captureDate === today) {
          todayEarnings += estimated;
        }

        items.push({
          s3_key: key,
          s3_prefix: prefix,
          capture_date: captureDate,
          duration_minutes: Number(durationMinutes.toFixed(2)),
          estimated_earning: estimated,
          raw_estimated_earning_before_review: calculateEstimatedEarning(metadata),
          status: review.status,
          payable: review.payable,
          review_required: review.review_required,
          quality_bucket: review.quality_bucket,
          reject_reasons: review.reasons,
          review_warnings: review.warnings,
          upload_required_complete: uploadStatus.requiredComplete,
          upload_server_marked_complete: uploadStatus.serverMarkedComplete,
          upload_strict_complete: uploadStatus.strictComplete,
          upload_missing_files: uploadStatus.missing,
        });
      } catch (itemError) {
        console.error('EARNINGS_ITEM_ERROR', {
          key,
          error: itemError.message,
        });
      }
    }

    return res.json({
      success: true,
      hunter_id: hunterId,
      currency: 'USD',
      today_earnings: Number(todayEarnings.toFixed(2)),
      pending: Number(total.toFixed(2)),
      available: 0,
      total: Number(total.toFixed(2)),
      matched_uploads: matchedUploads,
      payable_uploads: payableUploads,
      hold_uploads: holdUploads,
      rejected_uploads: rejectedUploads,
      skipped_incomplete_uploads: skippedIncompleteUploads,
      status: 'auto_review_applied_server_verified_uploads_only',
      auto_review_rules: {
        minimum_duration_minutes: 3,
        reject_short_video: true,
        reject_incomplete_upload: true,
        reject_missing_video: true,
        reject_missing_capture_metadata: true,
        reject_missing_imu_metadata: true,
        reject_stationary_or_no_movement: true,
        reject_high_duplicate_risk: true,
        reject_dark_low_sale_probability: true,
        reject_gps_reject_recommended: true,
      },
      items,
    });
  } catch (error) {
    console.error('EARNINGS_API_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to calculate hunter earnings',
    });
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
    let approvedCandidateUploads = 0;
    let holdUploads = 0;
    let rejectedUploads = 0;
    let skippedIncompleteUploads = 0;

    const rejectedReasons = {};
    const weeklyMap = {};
    const earningsMap = {};

    for (let i = 6; i >= 0; i--) {
      const date = dateKstDaysAgo(i);
      weeklyMap[date] = 0;
      earningsMap[date] = 0;
    }

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
        const review = getAutoReview(metadata, {
          uploadComplete,
          uploadMissing: uploadStatus.missing,
        });
        const estimated = calculatePayableEarning(metadata, review);

        totalUploads += 1;
        totalEarnings += estimated;

        if (review.status === 'REJECT') {
          rejectedUploads += 1;
          for (const reason of review.reasons) {
            rejectedReasons[reason] = (rejectedReasons[reason] || 0) + 1;
          }
        } else if (review.status === 'HOLD') {
          holdUploads += 1;
        } else {
          approvedCandidateUploads += 1;
        }

        if (captureDate === today) {
          todayUploads += 1;
          todayEarnings += estimated;
        }

        if (captureDate && Object.prototype.hasOwnProperty.call(weeklyMap, captureDate)) {
          weeklyMap[captureDate] += 1;
          earningsMap[captureDate] += estimated;
        }
      } catch (itemError) {
        console.error('DASHBOARD_ITEM_ERROR', {
          key,
          error: itemError.message,
        });
      }
    }

    const weeklyUploads = Object.entries(weeklyMap).map(([date, count]) => ({
      date,
      count,
    }));

    const earningsTrend = Object.entries(earningsMap).map(([date, amount]) => ({
      date,
      amount: Number(amount.toFixed(2)),
    }));

    totalEarnings = Number(totalEarnings.toFixed(2));
    todayEarnings = Number(todayEarnings.toFixed(2));

    const approvalRate =
      totalUploads > 0
        ? Number(((approvedCandidateUploads / totalUploads) * 100).toFixed(1))
        : 0;

    return res.json({
      success: true,
      hunter_id: hunterId,
      currency: 'USD',
      earnings: {
        today: todayEarnings,
        pending: totalEarnings,
        available: 0,
        total: totalEarnings,
      },
      todayActivity: {
        uploaded: todayUploads,
        approved: approvedCandidateUploads,
        hold: holdUploads,
        rejected: rejectedUploads,
      },
      performance: {
        approvalRate,
        qualityScore: approvalRate,
      },
      rejectedReasons,
      weeklyUploads,
      earningsTrend,
      raw: {
        today_uploads: todayUploads,
        total_uploads: totalUploads,
        today_earnings: todayEarnings,
        pending_uploads: approvedCandidateUploads,
        approved_uploads: approvedCandidateUploads,
        hold_uploads: holdUploads,
        rejected_uploads: rejectedUploads,
        skipped_incomplete_uploads: skippedIncompleteUploads,
        status: 'auto_review_applied_server_verified_uploads_only',
      },
    });
  } catch (error) {
    console.error('DASHBOARD_API_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to calculate hunter dashboard',
    });
  }
});

app.get('/hunter/rankings', async (req, res) => {
  try {
    const type = String(req.query.type || 'GLOBAL').toUpperCase();
    const currentHunterId = req.query.hunter_id || req.query.hunterId || null;

    const keys = await listCaptureMetadataKeys();
    const hunterMap = {};
    let skippedIncompleteUploads = 0;

    for (const key of keys) {
      try {
        const prefix = getCapturePrefixFromMetadataKey(key);
        const uploadStatus = await getUploadFileStatus(prefix);
        const uploadComplete = uploadStatus.strictComplete;

        if (!uploadComplete) {
          skippedIncompleteUploads += 1;
        }

        const metadata = await readJsonFromS3(key);
        const hunterId = extractHunterId(metadata, key);

        if (!isSafeId(hunterId)) continue;

        const review = getAutoReview(metadata, {
          uploadComplete,
          uploadMissing: uploadStatus.missing,
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
            approval_rate: 0,
            quality_score: 0,
          };
        }

        hunterMap[hunterId].total_uploads += 1;
        hunterMap[hunterId].total_earnings += estimated;
        hunterMap[hunterId].total_minutes += durationMinutes;

        if (review.status === 'REJECT') {
          hunterMap[hunterId].rejected_uploads += 1;
        } else if (review.status === 'HOLD') {
          hunterMap[hunterId].hold_uploads += 1;
        } else {
          hunterMap[hunterId].payable_uploads += 1;
        }
      } catch (itemError) {
        console.error('RANKINGS_ITEM_ERROR', {
          key,
          error: itemError.message,
        });
      }
    }

    const rankings = Object.values(hunterMap)
      .map((item) => {
        const approvalRate =
          item.total_uploads > 0
            ? Number(((item.payable_uploads / item.total_uploads) * 100).toFixed(1))
            : 0;

        return {
          ...item,
          total_earnings: Number(item.total_earnings.toFixed(2)),
          total_minutes: Number(item.total_minutes.toFixed(2)),
          approval_rate: approvalRate,
          quality_score: approvalRate,
          tier: getTierByEarnings(item.total_earnings),
        };
      })
      .sort((a, b) => {
        if (b.total_earnings !== a.total_earnings) {
          return b.total_earnings - a.total_earnings;
        }
        if (b.payable_uploads !== a.payable_uploads) {
          return b.payable_uploads - a.payable_uploads;
        }
        if (b.total_uploads !== a.total_uploads) {
          return b.total_uploads - a.total_uploads;
        }
        return a.hunter_id.localeCompare(b.hunter_id);
      })
      .map((item, index) => {
        const isCurrentHunter = currentHunterId && item.hunter_id === currentHunterId;

        return {
          rank: index + 1,
          id: item.hunter_id,
          hunter_id: item.hunter_id,
          displayName: isCurrentHunter ? 'You' : item.hunter_id,
          isCurrentHunter: Boolean(isCurrentHunter),
          tier: item.tier,
          approvalRate: item.approval_rate,
          rate: `${item.approval_rate}%`,
          totalUploads: item.total_uploads,
          payableUploads: item.payable_uploads,
          holdUploads: item.hold_uploads,
          rejectedUploads: item.rejected_uploads,
          totalEarnings: item.total_earnings,
          totalMinutes: item.total_minutes,
        };
      });

    const myRank = currentHunterId
      ? rankings.find((item) => item.hunter_id === currentHunterId) || null
      : null;

    return res.json({
      success: true,
      type,
      currency: 'USD',
      rankings,
      myRank,
      totalHunters: rankings.length,
      skipped_incomplete_uploads: skippedIncompleteUploads,
      status: 'auto_review_applied_server_verified_uploads_only',
    });
  } catch (error) {
    console.error('RANKINGS_API_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to calculate hunter rankings',
    });
  }
});

app.post('/api/v1/s3-presign', async (req, res) => {
  try {
    console.log('PRESIGN_REQUEST_RECEIVED', {
      time: new Date().toISOString(),
      body: req.body,
    });

    const incomingPrefix = req.body?.prefix;

    let hunterId =
      req.body?.hunterId ||
      req.body?.hunter_id ||
      req.body?.metadata?.hunter_id ||
      req.body?.metadata?.hunter?.hunter_id;

    let captureId =
      req.body?.captureId ||
      req.body?.capture_id;

    let prefix;

    if (incomingPrefix) {
      const parsed = normalizePrefix(incomingPrefix);

      if (!parsed) {
        return res.status(400).json({
          success: false,
          message: 'Invalid S3 prefix format',
        });
      }

      hunterId = parsed.hunterId;
      captureId = parsed.captureId;
      prefix = parsed.prefix;
    } else {
      if (!isSafeId(hunterId) || !isSafeId(captureId)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid hunterId or captureId',
        });
      }

      const date = todayKst();
      prefix = buildCapturePrefix({ date, hunterId, captureId });
    }

    const allowedFiles = {
      video: {
        fileName: REQUIRED_UPLOAD_FILES.video,
        contentType: 'video/mp4',
      },
      captureMetadata: {
        fileName: REQUIRED_UPLOAD_FILES.captureMetadata,
        contentType: 'application/json',
      },
      imuMetadata: {
        fileName: REQUIRED_UPLOAD_FILES.imuMetadata,
        contentType: 'application/json',
      },
    };

    const makeUrl = async ({ fileName, contentType }) => {
      const command = new PutObjectCommand({
        Bucket: BUCKET,
        Key: `${prefix}${fileName}`,
        ContentType: contentType,
      });

      return await getSignedUrl(s3, command, {
        expiresIn: 300,
      });
    };

    const urls = {
      video: await makeUrl(allowedFiles.video),
      captureMetadata: await makeUrl(allowedFiles.captureMetadata),
      imuMetadata: await makeUrl(allowedFiles.imuMetadata),
    };

    console.log('PRESIGN_URLS_CREATED', {
      time: new Date().toISOString(),
      prefix,
      hunterId,
      captureId,
      uploadCompleteMode: 'server_verified_only',
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      s3Prefix: prefix,
      requiredFiles: REQUIRED_UPLOAD_FILES,
      uploadCompleteMode: 'server_verified_only',
      finalizeEndpoint: '/api/v1/upload-complete',
      urls,
    });
  } catch (error) {
    console.error('PRESIGN_ERROR:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to create presigned URLs',
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
        error: 'UNDER_3_MINUTES',
        message: 'Video duration must be at least 3 minutes before completion',
        duration_minutes: Number(durationMinutes.toFixed(2)),
        minimum_duration_minutes: 3,
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

app.listen(PORT, '0.0.0.0', () => {
  console.log(`S3 Presign Server running on http://0.0.0.0:${PORT}`);
});
