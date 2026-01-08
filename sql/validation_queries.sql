-- ============================================================================
-- Tenna Lakehouse Data Validation Queries
-- ============================================================================
-- Practical queries using all 14 tables in your lakehouse

-- ============================================================================
-- SIMPLE QUERIES - Quick Health Checks
-- ============================================================================

-- 1. Row counts for ALL tables
SELECT 'assets' as table_name, COUNT(*) as row_count FROM assets
UNION ALL
SELECT 'asset_financials', COUNT(*) FROM asset_financials
UNION ALL
SELECT 'asset_assignee_history', COUNT(*) FROM asset_assignee_history
UNION ALL
SELECT 'asset_label_associations', COUNT(*) FROM asset_label_associations
UNION ALL
SELECT 'asset_dt_codes', COUNT(*) FROM asset_dt_codes
UNION ALL
SELECT 'asset_labels', COUNT(*) FROM asset_labels
UNION ALL
SELECT 'asset_organization_history', COUNT(*) FROM asset_organization_history
UNION ALL
SELECT 'asset_registrations', COUNT(*) FROM asset_registrations
UNION ALL
SELECT 'asset_site_history', COUNT(*) FROM asset_site_history
UNION ALL
SELECT 'asset_warranties', COUNT(*) FROM asset_warranties
UNION ALL
SELECT 'asset_readings_daily', COUNT(*) FROM asset_readings_daily
UNION ALL
SELECT 'asset_utilizations_daily', COUNT(*) FROM asset_utilizations_daily
UNION ALL
SELECT 'asset_site_daily_utilizations', COUNT(*) FROM asset_site_daily_utilizations
UNION ALL
SELECT 'asset_net_working_hours_daily', COUNT(*) FROM asset_net_working_hours_daily
ORDER BY row_count DESC;

-- 2. Check for duplicate asset IDs
SELECT 
    asset_id,
    COUNT(*) as duplicate_count
FROM assets
GROUP BY asset_id
HAVING COUNT(*) > 1;

-- 3. Check for null values in key fields
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) as null_ids,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) as null_names,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as null_status
FROM assets;

-- 4. Check data freshness across tables
SELECT 
    'assets' as table_name,
    MAX(updated_at) as most_recent_update,
    DATEDIFF(hour, MAX(updated_at), GETDATE()) as hours_old
FROM assets
UNION ALL
SELECT 
    'asset_financials',
    MAX(updated_at),
    DATEDIFF(hour, MAX(updated_at), GETDATE())
FROM asset_financials
UNION ALL
SELECT
    'asset_warranties',
    MAX(updated_at),
    DATEDIFF(hour, MAX(updated_at), GETDATE())
FROM asset_warranties
ORDER BY hours_old ASC;

-- 5. View recent asset updates
SELECT TOP 10 
    asset_id,
    name,
    make,
    model,
    status,
    updated_at
FROM assets
ORDER BY updated_at DESC;

-- 6. Assets by status distribution
SELECT 
    status,
    COUNT(*) as asset_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM assets
GROUP BY status
ORDER BY asset_count DESC;

-- 7. Active diagnostic trouble codes by asset
SELECT 
    asset_id,
    COUNT(*) as active_dt_codes,
    MIN(timestamp) as earliest_code,
    MAX(timestamp) as latest_code
FROM asset_dt_codes
WHERE is_acknowledged = false
GROUP BY asset_id
ORDER BY active_dt_codes DESC;

-- 8. Assets with expired warranties
SELECT 
    asset_id,
    COUNT(*) as warranty_count,
    MAX(end_value_date) as latest_expiration
FROM asset_warranties
WHERE expired = true
GROUP BY asset_id
HAVING COUNT(*) > 0
ORDER BY latest_expiration DESC;

-- 9. Label usage across assets
SELECT 
    al.label_name,
    al.label_color,
    COUNT(DISTINCT ala.asset_id) as assets_with_label,
    al.deactivated
FROM asset_labels al
LEFT JOIN asset_label_associations ala ON al.asset_label_id = ala.asset_label_id
    AND ala.currently_applied = true
GROUP BY al.label_name, al.label_color, al.deactivated
ORDER BY assets_with_label DESC;

-- 10. Compare working hours vs idle hours (last 30 days)
WITH hourly_analysis AS (
    SELECT 
        asset_id,
        date,
        working_seconds / 3600.0 as working_hours,
        total_idle_duration_seconds / 3600.0 as idle_hours,
        total_duration_seconds / 3600.0 as total_hours,
        CASE 
            WHEN total_duration_seconds > 0 
            THEN (working_seconds * 100.0 / total_duration_seconds)
            ELSE 0 
        END as efficiency_percentage
    FROM asset_net_working_hours_daily
    WHERE date >= DATEADD(day, -30, GETDATE())
)
SELECT 
    asset_id,
    COUNT(*) as days_tracked,
    ROUND(AVG(working_hours), 2) as avg_working_hours,
    ROUND(AVG(idle_hours), 2) as avg_idle_hours,
    ROUND(AVG(efficiency_percentage), 2) as avg_efficiency_pct,
    ROUND(SUM(working_hours), 2) as total_working_hours
FROM hourly_analysis
GROUP BY asset_id
ORDER BY avg_efficiency_pct DESC;

-- 11. Organization changes frequency
SELECT 
    asset_id,
    COUNT(*) as org_changes,
    MIN(from_date) as first_org_change,
    MAX(to_date) as last_org_change,
    DATEDIFF(month, MIN(from_date), MAX(to_date)) as months_tracked,
    CASE 
        WHEN DATEDIFF(month, MIN(from_date), MAX(to_date)) > 0
        THEN COUNT(*) * 1.0 / DATEDIFF(month, MIN(from_date), MAX(to_date))
        ELSE 0
    END as changes_per_month
FROM asset_organization_history
GROUP BY asset_id
HAVING COUNT(*) > 3
ORDER BY changes_per_month DESC;

-- 12. Assets with registrations, warranties, and financial data
SELECT 
    a.asset_id,
    a.name,
    a.make,
    a.model,
    CASE WHEN af.asset_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_financials,
    CASE WHEN ar.asset_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_registration,
    CASE WHEN aw.asset_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_warranty,
    COUNT(DISTINCT aw.asset_warranty_id) as warranty_count
FROM assets a
LEFT JOIN asset_financials af ON a.asset_id = af.asset_id
LEFT JOIN asset_registrations ar ON a.asset_id = ar.asset_id
LEFT JOIN asset_warranties aw ON a.asset_id = aw.asset_id AND aw.expired = false
GROUP BY a.asset_id, a.name, a.make, a.model, 
    CASE WHEN af.asset_id IS NOT NULL THEN 'Yes' ELSE 'No' END,
    CASE WHEN ar.asset_id IS NOT NULL THEN 'Yes' ELSE 'No' END,
    CASE WHEN aw.asset_id IS NOT NULL THEN 'Yes' ELSE 'No' END
ORDER BY a.name;

-- 13. Current assignments with utilization and DT codes
SELECT TOP 50
    a.asset_id,
    a.name,
    ah.assignee_name,
    ah.assignment_start,
    AVG(u.utilization_percentage) as avg_utilization_30days,
    COUNT(DISTINCT dt.asset_dt_code_id) as active_trouble_codes,
    MAX(u.date) as last_utilization_date
FROM assets a
INNER JOIN asset_assignee_history ah ON a.asset_id = ah.asset_id
LEFT JOIN asset_utilizations_daily u ON a.asset_id = u.asset_id
    AND u.date >= DATEADD(day, -30, GETDATE())
LEFT JOIN asset_dt_codes dt ON a.asset_id = dt.asset_id
    AND dt.is_acknowledged = 0
WHERE ah.currently_assigned = 1
GROUP BY a.asset_id, a.name, ah.assignee_name, ah.assignment_start
ORDER BY active_trouble_codes DESC, avg_utilization_30days ASC;

-- 14. Site history with daily utilization rollup
SELECT 
    sh.site_name,
    sh.asset_id,
    a.name as asset_name,
    sh.enter_date,
    sh.exit_date,
    DATEDIFF(day, sh.enter_date, COALESCE(sh.exit_date, GETDATE())) as days_at_site,
    AVG(sdu.active_run_seconds) / 3600.0 as avg_daily_hours,
    SUM(sdu.active_run_seconds) / 3600.0 as total_hours_at_site
FROM asset_site_history sh
INNER JOIN assets a ON sh.asset_id = a.asset_id
LEFT JOIN asset_site_daily_utilizations sdu ON sh.asset_id = sdu.asset_id
    AND sdu.date >= sh.enter_date
    AND (sh.exit_date IS NULL OR sdu.date <= sh.exit_date)
GROUP BY sh.site_name, sh.asset_id, a.name, sh.enter_date, sh.exit_date
ORDER BY total_hours_at_site DESC NULLS LAST;

-- 15. Assets with labels and their performance
SELECT 
    al.label_name,
    COUNT(DISTINCT a.asset_id) as asset_count,
    AVG(u.utilization_percentage) as avg_utilization,
    AVG(nwh.working_percentage) as avg_working_pct,
    COUNT(DISTINCT dt.asset_dt_code_id) as total_dt_codes
FROM asset_labels al
INNER JOIN asset_label_associations ala ON al.asset_label_id = ala.asset_label_id
INNER JOIN assets a ON ala.asset_id = a.asset_id
LEFT JOIN asset_utilizations_daily u ON a.asset_id = u.asset_id
    AND u.date >= DATEADD(day, -30, GETDATE())
LEFT JOIN asset_net_working_hours_daily nwh ON a.asset_id = nwh.asset_id
    AND nwh.date >= DATEADD(day, -30, GETDATE())
LEFT JOIN asset_dt_codes dt ON a.asset_id = dt.asset_id
    AND dt.is_acknowledged = false
WHERE ala.currently_applied = true
GROUP BY al.label_name
ORDER BY avg_utilization DESC;

-- 16. Compare daily readings vs utilization consistency
SELECT 
    r.asset_id,
    r.date,
    r.cumulative_hours_end_of_day,
    u.utilization_percentage,
    u.active_run_seconds / 3600.0 as active_hours,
    nwh.working_seconds / 3600.0 as working_hours,
    nwh.total_idle_duration_seconds / 3600.0 as idle_hours
FROM asset_readings_daily r
INNER JOIN asset_utilizations_daily u ON r.asset_id = u.asset_id 
    AND r.date = u.date
INNER JOIN asset_net_working_hours_daily nwh ON r.asset_id = nwh.asset_id
    AND r.date = nwh.date
WHERE r.date >= DATEADD(day, -7, GETDATE())
ORDER BY r.asset_id, r.date DESC;

-- 17. Check pipeline execution history
SELECT 
    pipeline_name,
    last_run_time,
    status,
    DATEDIFF(hour, last_run_time, GETDATE()) as hours_since_run
FROM control_last_run
ORDER BY last_run_time DESC;

-- 18. Assets missing from daily tracking tables
SELECT 
    a.asset_id,
    a.name,
    a.status,
    MAX(r.date) as last_reading_date,
    MAX(u.date) as last_utilization_date,
    MAX(nwh.date) as last_working_hours_date,
    DATEDIFF(day, MAX(r.date), GETDATE()) as days_since_reading
FROM assets a
LEFT JOIN asset_readings_daily r ON a.asset_id = r.asset_id
LEFT JOIN asset_utilizations_daily u ON a.asset_id = u.asset_id
LEFT JOIN asset_net_working_hours_daily nwh ON a.asset_id = nwh.asset_id
WHERE a.status = 'Active'
GROUP BY a.asset_id, a.name, a.status
HAVING MAX(r.date) < DATEADD(day, -7, GETDATE())
    OR MAX(r.date) IS NULL
ORDER BY days_since_reading DESC;
