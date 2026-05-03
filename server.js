require('dotenv').config();

const express = require('express');
const cors = require('cors');
const {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  GetObjectCommand,
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

app.use(express.json({ limit: '1mb' }));

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

const listCaptureMetadataKeys = async () => {
  const keys = [];
  let continuationToken;

  do {
    const result = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: 'real/v1/raw/',
        ContinuationToken: continuationToken,
      })
    );

    const foundKeys = (result.Contents || [])
      .map((item) => item.Key)
      .filter((key) => key && key.endsWith('capture_metadata_v1.json'));

    keys.push(...foundKeys);
    continuationToken = result.NextContinuationToken;
  } while (continuationToken);

  return keys;
};

const extractHunterId = (metadata, key) => {
  let metadataHunterId =
    metadata.hunter_id ||
    metadata.hunterId ||
    metadata.hunter?.hunter_id ||
    metadata.hunter?.hunterId;

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
    metadata.recording_started_at ||
    metadata.recordingStartTime ||
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
    Number(metadata.video_duration_ms) ||
    Number(metadata.duration_ms) ||
    Number(metadata.recording_duration_ms) ||
    0;

  return Math.max(0, ms / 1000 / 60);
};

const getQualityMultiplier = (metadata) => {
  const score =
    Number(metadata.quality_score) ||
    Number(metadata.overall_quality_score) ||
    Number(metadata.dataset_quality_score) ||
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

const getTierByEarnings = (totalEarnings) => {
  if (totalEarnings >= 50) return 'DIAMOND HUNTER';
  if (totalEarnings >= 20) return 'GOLD HUNTER';
  if (totalEarnings >= 5) return 'SILVER HUNTER';
  return 'BRONZE HUNTER';
};

app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 's3-presign-server',
    bucket: BUCKET,
    region: REGION,
  });
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

    const items = [];

    for (const key of keys) {
      try {
        const metadata = await readJsonFromS3(key);
        const metadataHunterId = extractHunterId(metadata, key);

        if (metadataHunterId !== hunterId) continue;

        const estimated = calculateEstimatedEarning(metadata);
        const captureDate = extractCaptureDate(metadata, key);
        const durationMinutes = getDurationMinutes(metadata);

        matchedUploads += 1;
        total += estimated;

        if (captureDate === today) {
          todayEarnings += estimated;
        }

        items.push({
          s3_key: key,
          capture_date: captureDate,
          duration_minutes: Number(durationMinutes.toFixed(2)),
          estimated_earning: estimated,
          status: 'estimated_pending',
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
      status: 'estimated_only',
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

    const weeklyMap = {};
    const earningsMap = {};

    for (let i = 6; i >= 0; i--) {
      const date = dateKstDaysAgo(i);
      weeklyMap[date] = 0;
      earningsMap[date] = 0;
    }

    for (const key of keys) {
      try {
        const metadata = await readJsonFromS3(key);
        const metadataHunterId = extractHunterId(metadata, key);

        if (metadataHunterId !== hunterId) continue;

        const captureDate = extractCaptureDate(metadata, key);
        const estimated = calculateEstimatedEarning(metadata);

        totalUploads += 1;
        totalEarnings += estimated;

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
        approved: 0,
        rejected: 0,
      },
      performance: {
        approvalRate: 0,
        qualityScore: 0,
      },
      rejectedReasons: {
        tooShaky: 0,
        tooDark: 0,
        noTrafficScene: 0,
      },
      weeklyUploads,
      earningsTrend,
      raw: {
        today_uploads: todayUploads,
        total_uploads: totalUploads,
        today_earnings: todayEarnings,
        pending_uploads: totalUploads,
        approved_uploads: 0,
        rejected_uploads: 0,
        status: 'estimated_pending_before_review',
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

    for (const key of keys) {
      try {
        const metadata = await readJsonFromS3(key);
        const hunterId = extractHunterId(metadata, key);

        if (!isSafeId(hunterId)) continue;

        const estimated = calculateEstimatedEarning(metadata);
        const durationMinutes = getDurationMinutes(metadata);

        if (!hunterMap[hunterId]) {
          hunterMap[hunterId] = {
            hunter_id: hunterId,
            total_uploads: 0,
            total_earnings: 0,
            total_minutes: 0,
            approval_rate: 0,
            quality_score: 0,
          };
        }

        hunterMap[hunterId].total_uploads += 1;
        hunterMap[hunterId].total_earnings += estimated;
        hunterMap[hunterId].total_minutes += durationMinutes;
      } catch (itemError) {
        console.error('RANKINGS_ITEM_ERROR', {
          key,
          error: itemError.message,
        });
      }
    }

    const rankings = Object.values(hunterMap)
      .map((item) => ({
        ...item,
        total_earnings: Number(item.total_earnings.toFixed(2)),
        total_minutes: Number(item.total_minutes.toFixed(2)),
        tier: getTierByEarnings(item.total_earnings),
      }))
      .sort((a, b) => {
        if (b.total_earnings !== a.total_earnings) {
          return b.total_earnings - a.total_earnings;
        }
        if (b.total_uploads !== a.total_uploads) {
          return b.total_uploads - a.total_uploads;
        }
        return a.hunter_id.localeCompare(b.hunter_id);
      })
      .map((item, index) => ({
        rank: index + 1,
        id: item.hunter_id,
        hunter_id: item.hunter_id,
        tier: item.tier,
        approvalRate: item.approval_rate,
        rate: `${item.approval_rate}%`,
        totalUploads: item.total_uploads,
        totalEarnings: item.total_earnings,
        totalMinutes: item.total_minutes,
      }));

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
      status: 'estimated_pending_before_review',
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
      const match = incomingPrefix.match(
        /^(?:real\/)?v1\/raw\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/([A-Za-z0-9_-]{3,80})\/([A-Za-z0-9_-]{3,120})\/$/
      );

      if (!match) {
        return res.status(400).json({
          success: false,
          message: 'Invalid S3 prefix format',
        });
      }

      const dateFromPrefix = match[1];
      hunterId = match[2];
      captureId = match[3];

      prefix = `real/v1/raw/${dateFromPrefix}/${hunterId}/${captureId}/`;
    } else {
      if (!isSafeId(hunterId) || !isSafeId(captureId)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid hunterId or captureId',
        });
      }

      const date = todayKst();
      prefix = `real/v1/raw/${date}/${hunterId}/${captureId}/`;
    }

    const allowedFiles = {
      video: {
        fileName: 'video_raw.mp4',
        contentType: 'video/mp4',
      },
      captureMetadata: {
        fileName: 'capture_metadata_v1.json',
        contentType: 'application/json',
      },
      imuMetadata: {
        fileName: 'imu_metadata_v1.json',
        contentType: 'application/json',
      },
      uploadComplete: {
        fileName: 'upload_complete.json',
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
      uploadComplete: await makeUrl(allowedFiles.uploadComplete),
    };

    console.log('PRESIGN_URLS_CREATED', {
      time: new Date().toISOString(),
      prefix,
      hunterId,
      captureId,
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      s3Prefix: prefix,
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

app.listen(PORT, '0.0.0.0', () => {
  console.log(`S3 Presign Server running on http://0.0.0.0:${PORT}`);
});