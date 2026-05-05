require('dotenv').config();

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

app.get('/admin/review', async (req, res) => {
  res.send(`
  <html>
  <head>
    <title>Admin Review</title>
    <style>
      body { font-family: Arial; background:#111; color:#eee; }
      table { border-collapse: collapse; width: 100%; }
      td, th { border:1px solid #444; padding:8px; font-size:12px; }
      th { background:#222; }
      button { padding:5px 10px; cursor:pointer; }
      video { width:600px; margin-top:10px; }
    </style>
  </head>
  <body>
    <h2>📊 Hunter Review Dashboard</h2>

    <table id="table">
      <thead>
        <tr>
          <th>Hunter</th>
          <th>Phone</th>
          <th>Country</th>
          <th>Date</th>
          <th>Duration</th>
          <th>Status</th>
          <th>Reasons</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>

    <div id="player"></div>

    <script>
      async function load() {
        const res = await fetch('/admin/review-uploads?limit=50');
        const data = await res.json();

        const tbody = document.querySelector('tbody');

        data.items.forEach(item => {
          const tr = document.createElement('tr');

          tr.innerHTML = \`
            <td>\${item.hunter.nickname} (\${item.hunter_id})</td>
            <td>\${item.hunter.phone}</td>
            <td>\${item.hunter.country}</td>
            <td>\${item.capture_date}</td>
            <td>\${item.duration_minutes}</td>
            <td>\${item.status}</td>
            <td>\${item.reject_reasons.join(', ')}</td>
            <td><button onclick="play('\${item.video_key}')">보기</button></td>
          \`;

          tbody.appendChild(tr);
        });
      }

      async function play(key) {
        const res = await fetch('/admin/video-url?key=' + encodeURIComponent(key));
        const data = await res.json();

        document.getElementById('player').innerHTML = \`
          <video controls src="\${data.url}"></video>
        \`;
      }

      load();
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
    minimum_duration_minutes: 3,
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

app.post('/api/v1/s3-multipart/create', requireSupportedAppVersion, async (req, res) => {
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

app.post('/api/v1/s3-multipart/part-url', requireSupportedAppVersion, async (req, res) => {
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

app.post('/api/v1/s3-multipart/complete', requireSupportedAppVersion, async (req, res) => {
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

app.post('/api/v1/upload-complete', requireSupportedAppVersion, async (req, res) => {
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
    description: "Record moving traffic for at least 3 minutes.",
    minimum_minutes: 3,
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