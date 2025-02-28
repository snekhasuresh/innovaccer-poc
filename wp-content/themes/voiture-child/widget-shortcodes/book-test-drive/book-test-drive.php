<?php
// Add AJAX action to get models by make
add_action('wp_ajax_get_models_by_make', 'get_models_by_make');

function get_models_by_make()
{
    check_ajax_referer('filter_test_drive_requests', 'nonce');

    if (!current_user_can('manage_options')) {
        wp_send_json_error('Permission denied');
    }

    $make_term_id = isset($_POST['make_term_id']) ? intval($_POST['make_term_id']) : 0;

    if (!$make_term_id) {
        wp_send_json_error('Invalid make');
    }

    global $wpdb;

    // Debug output
    error_log('Searching models for make_term_id: ' . $make_term_id);

    // Get models where the make matches the selected term_id
    $query = $wpdb->prepare(
        "SELECT DISTINCT p.post_title 
        FROM {$wpdb->posts} p
        INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
        WHERE pm.meta_key = '%s'
        AND pm.meta_value = %d
        AND p.post_type = 'listing'
        AND p.post_status = 'publish'
        ORDER BY p.post_title ASC",
        '_listing_make',
        $make_term_id
    );

    error_log('Model Query: ' . $query);

    $models = $wpdb->get_col($query);

    wp_send_json_success($models);
}

function display_test_drive_requests()
{
    if (!current_user_can('manage_options')) {
        return '<p>You do not have permission to view this content.</p>';
    }

    global $wpdb;
    $table_name = $wpdb->prefix . 'book_test_drive_requests';

    // Process filters only if form is submitted
    if (isset($_GET['filter_submit'])) {
        if (!isset($_GET['filter_nonce']) || !wp_verify_nonce($_GET['filter_nonce'], 'filter_test_drive_requests')) {
            return '<p>Security check failed. Please refresh the page and try again.</p>';
        }
    }

    // Get and sanitize filter values
    $start_date = isset($_GET['start_date']) ? sanitize_text_field($_GET['start_date']) : '';
    $end_date = isset($_GET['end_date']) ? sanitize_text_field($_GET['end_date']) : '';
    $filter_make = isset($_GET['filter_make']) ? intval($_GET['filter_make']) : '';
    $filter_model = isset($_GET['filter_model']) ? sanitize_text_field($_GET['filter_model']) : '';

    $filter_model_original = $filter_model;
    $make_words = explode(' ', $filter_make);
    $filter_model = strtolower($filter_model);
    $make_model_array = explode(' ', $filter_model);
    $filter_model = implode(' ', array_slice($make_model_array, count($make_words)));

    $filter_model_original = $filter_model;
    $make_words = explode(' ', $filter_make);
    $filter_model = strtolower($filter_model);
    $make_model_array = explode(' ', $filter_model);
    $filter_model = implode('-', array_slice($make_model_array, count($make_words)));

    // Build query with prepared statements
    $query = "SELECT * FROM $table_name WHERE 1=1";
    $prepare_values = array();

    if (!empty($start_date)) {
        $query .= " AND DATE(created_at) >= %s";
        $prepare_values[] = $start_date;
    }
    if (!empty($end_date)) {
        $query .= " AND DATE(created_at) <= %s";
        $prepare_values[] = $end_date;
    }
    if (!empty($filter_make)) {
        // Join with terms to get make name
        $make_term = get_term($filter_make, 'listing_make');
        if ($make_term && !is_wp_error($make_term)) {
            $query .= " AND LOWER(make) = LOWER(%s)";
            $prepare_values[] = $make_term->name;
        }
    }
    if (!empty($filter_model_original)) {
        $query .= " AND LOWER(model) = LOWER(%s)";
        $prepare_values[] = $filter_model;
    }

    $query .= " ORDER BY created_at DESC";

    // Prepare and execute query
    $requests = !empty($prepare_values)
        ? $wpdb->get_results($wpdb->prepare($query, $prepare_values))
        : $wpdb->get_results($query);

    // Get makes from taxonomy with direct SQL query
    $makes_query = $wpdb->prepare(
        "SELECT t.term_id, t.name 
        FROM {$wpdb->terms} t 
        INNER JOIN {$wpdb->term_taxonomy} tt ON t.term_id = tt.term_id 
        WHERE tt.taxonomy = %s 
        ORDER BY t.name ASC",
        'listing_make'
    );
    $makes = $wpdb->get_results($makes_query);

    // Enqueue necessary scripts
    wp_enqueue_script('jquery');

    // Generate nonce for AJAX
    $ajax_nonce = wp_create_nonce('filter_test_drive_requests');

    ob_start();
?>
    <div class="test-drive-requests">
        <h2>Test Drive Requests</h2>

        <!-- Filter Form -->
        <form method="get" class="test-drive-filters" style="margin-bottom: 20px; padding: 15px; background: #f5f5f5; border: 1px solid #ddd; border-radius: 4px;">
            <?php wp_nonce_field('filter_test_drive_requests', 'filter_nonce'); ?>
            <input type="hidden" name="page" value="test-drive-requests">

            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 15px;">
                <div>
                    <label for="start_date">Start Date:</label>
                    <input type="date" id="start_date" name="start_date" value="<?php echo esc_attr($start_date); ?>" style="width: 100%;">
                </div>

                <div>
                    <label for="end_date">End Date:</label>
                    <input type="date" id="end_date" name="end_date" value="<?php echo esc_attr($end_date); ?>" style="width: 100%;">
                </div>

                <div>
                    <label for="filter_make">Make:</label>
                    <select id="filter_make" name="filter_make" style="width: 100%;">
                        <option value="">All Makes</option>
                        <?php foreach ($makes as $make) : ?>
                            <option value="<?php echo esc_attr($make->term_id); ?>" <?php selected($filter_make, $make->term_id); ?>>
                                <?php echo esc_html($make->name); ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </div>

                <div>
                    <label for="filter_model">Model:</label>
                    <select id="filter_model" name="filter_model" style="width: 100%;" <?php echo empty($filter_make) ? 'disabled' : ''; ?>>
                        <option value="">Select Model</option>
                        <?php if (!empty($filter_make)) :
                            $models = $wpdb->get_col($wpdb->prepare(
                                "SELECT DISTINCT p.post_title 
                                FROM {$wpdb->posts} p
                                INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
                                WHERE pm.meta_key = %s 
                                AND pm.meta_value = %d
                                AND p.post_type = 'listing'
                                AND p.post_status = 'publish'
                                ORDER BY p.post_title ASC",
                                '_listing_make',
                                $filter_make
                            ));
                            foreach ($models as $model) : ?>
                                <option value="<?php echo esc_attr($model); ?>" <?php selected($filter_model_original, $model); ?>>
                                    <?php echo esc_html($model); ?>
                                </option>
                        <?php endforeach;
                        endif; ?>
                    </select>
                </div>
            </div>

            <div style="display: flex; gap: 10px;">
                <button type="submit" name="filter_submit" value="1" class="button button-primary">Apply Filters</button>
                <a href="<?php echo admin_url('admin.php?page=test-drive-requests'); ?>" class="button">Reset Filters</a>
            </div>
        </form>

        <td colspan="6">
            <button class="button button-primary" onclick="exportCSV()">Export to CSV</button>
        </td>
        <table class="wp-list-table widefat fixed striped" style="margin-top: 15px;">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Email</th>
                    <th>Phone</th>
                    <th>Make</th>
                    <th>Model</th>
                    <th>Date</th>
                </tr>
            </thead>
            <tbody>
                <?php if ($requests) : ?>
                    <?php foreach ($requests as $request) : ?>
                        <tr>
                            <td><?php echo esc_html($request->name); ?></td>
                            <td><?php echo esc_html($request->email); ?></td>
                            <td><?php echo esc_html($request->phone); ?></td>
                            <td><?php echo esc_html($request->make); ?></td>
                            <td><?php echo esc_html($request->model); ?></td>
                            <td><?php echo esc_html($request->created_at); ?></td>
                        </tr>
                    <?php endforeach; ?>
                    <tr>
                    <?php else : ?>
                    <tr>
                        <td colspan="6">No test drive requests found.</td>
                    </tr>
                <?php endif; ?>
            </tbody>
        </table>
    </div>

    <script type="text/javascript">
        jQuery(document).ready(function($) {
            var ajaxNonce = '<?php echo $ajax_nonce; ?>';

            $('#filter_make').on('change', function() {
                var makeId = $(this).val();
                var modelSelect = $('#filter_model');

                modelSelect.prop('disabled', !makeId);
                modelSelect.html('<option value="">Select Model</option>');

                if (makeId) {
                    $.ajax({
                        url: ajaxurl,
                        type: 'POST',
                        data: {
                            action: 'get_models_by_make',
                            make_term_id: makeId,
                            nonce: ajaxNonce
                        },
                        success: function(response) {
                            console.log('AJAX Response:', response);
                            if (response.success && response.data) {
                                response.data.forEach(function(model) {
                                    modelSelect.append($('<option></option>')
                                        .attr('value', model)
                                        .text(model));
                                });
                            }
                        },
                        error: function(xhr, status, error) {
                            console.error('AJAX Error:', error); // Debug log
                        }
                    });
                }
            });
        });

        function exportCSV() {
            window.location.href = '<?php echo admin_url('admin-ajax.php?action=export_test_drive_requests'); ?>';
        }
    </script>
<?php

    return ob_get_clean();
}
add_shortcode('test_drive_requests', 'display_test_drive_requests');

function export_test_drive_requests()
{
    if (!current_user_can('manage_options')) {
        wp_die('Permission denied');
    }

    global $wpdb;
    $table_name = $wpdb->prefix . 'book_test_drive_requests';

    $requests = $wpdb->get_results("SELECT * FROM $table_name ORDER BY created_at DESC");

    $filename = 'test-drive-requests-' . date('Y-m-d') . '.csv';

    header('Content-Type: text/csv');
    header('Content-Disposition: attachment; filename="' . $filename . '"');
    header('Pragma: no-cache');
    header('Expires: 0');

    $output = fopen('php://output', 'w');

    fputcsv($output, array('Name', 'Email', 'Phone', 'Make', 'Model', 'Date'));

    foreach ($requests as $request) {
        fputcsv($output, array(
            $request->name,
            $request->email,
            $request->phone,
            $request->make,
            $request->model,
            $request->created_at
        ));
    }

    fclose($output);
    exit;
}
add_action('wp_ajax_export_test_drive_requests', 'export_test_drive_requests');

// Add menu item to admin dashboard
function register_test_drive_admin_page()
{
    add_menu_page(
        'Test Drive Requests',
        'Test Drive Requests',
        'manage_options',
        'test-drive-requests',
        'render_test_drive_requests_page',
        'dashicons-car',
        20
    );
}
add_action('admin_menu', 'register_test_drive_admin_page');

// Render the admin page
function render_test_drive_requests_page()
{
    echo '<div class="wrap">';
    echo '<h1>Test Drive Requests</h1>';
    echo do_shortcode('[test_drive_requests]');
    echo '</div>';
}
