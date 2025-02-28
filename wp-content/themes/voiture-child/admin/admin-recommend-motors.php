<?php
function motor_admin_menu()
{
    // Add the main menu page
    add_menu_page(
        'Recommended',
        'Recommended',
        'manage_options',
        'custom_menu',
        'menu_page_callback',
        'dashicons-menu',
        7
    );

    // Add the 'Top Motor Model' submenu page
    add_submenu_page(
        'custom_menu',
        'Top Motor Model',
        'Top Motor Model',
        'manage_options',
        'top_bike_model', // Menu slug
        'top_bike_model_page_callback'
    );

    // Add the 'Recommend Motor Model' submenu page
    add_submenu_page(
        'custom_menu',
        'Recommend Motor Model',
        'Recommend Motor Model',
        'manage_options',
        'recommend_bike_model',
        'recommend_bike_model_page_callback' // Callback function
    );
}
add_action('motor_admin_menu', 'motor_admin_menu');

//recommend bike listing page
function recommend_bike_model_page_callback()
{
    $recommend_bike_models = get_option('recommended_bike_models', []);
    $grouped_models = [];

    if (!empty($recommend_bike_models)) {
        foreach ($recommend_bike_models as $category_key => $data) {
            if (isset($data['category'], $data['bike_models'])) {
                $category_name = $data['category'];
                $weight = $data['weight'];
                $status = $data['status'] ? 'Enable' : 'Disabled';

                if (!isset($grouped_models[$category_name])) {
                    $grouped_models[$category_name] = [
                        'models' => [],
                        'weight' => $weight,
                        'status' => $status,
                    ];
                }

                foreach ($data['bike_models'] as $bike_model_data) {
                    $bike_model_id = isset($bike_model_data['id']) ? $bike_model_data['id'] : null;
                    if ($bike_model_id) {
                        $bike_model = get_post($bike_model_id);
                        if ($bike_model) {
                            $grouped_models[$category_name]['models'][] = $bike_model->post_title;
                        }
                    }
                }
            }
        }
    }
?>
    <div class="wrap">
        <h1>Recommended Motor Models</h1>
        <a href="<?php echo admin_url('admin.php?page=add-new-recommend-bike'); ?>" class="button button-primary" style="margin-bottom: 20px;">Add New Motor Model</a>
        <table class="wp-list-table widefat fixed striped">
            <thead>
                <tr>
                    <th>Category Name</th>
                    <th>Motor Models</th>
                    <th>Weight</th>
                    <th>Status</th>
                    <th>Operate</th>
                </tr>
            </thead>
            <tbody>
                <?php
                if (!empty($grouped_models)) {
                    foreach ($grouped_models as $category_name => $data) {
                        $bike_models_list = implode(', ', $data['models']);
                        echo '<tr>';
                        echo '<td>' . esc_html($category_name) . '</td>';
                        echo '<td>' . esc_html($bike_models_list) . '</td>';
                        echo '<td>' . esc_html($data['weight']) . '</td>';
                        echo '<td>' . esc_html($data['status']) . '</td>';
                        echo '<td>
                        <div style="display: inline-block; margin-right: 5px;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-recommend-bike&category=' . $category_name)) . '" class="button">View</a>
                        </div>
                        <div style="display: inline-block;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-recommend-bike&category=' . $category_name . '&edit=true')) . '" class="button">Edit</a>
                        </div>
                        <div style="display: inline-block; margin-right: 5px;">
                            <form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" onsubmit="return confirmDelete(\'' . esc_js($category_name) . '\');">
                                <input type="hidden" name="action" value="delete_recommend_bike_model">
                                <input type="hidden" name="category_name" value="' . esc_attr($category_name) . '">
                                <button type="submit" class="button button-secondary">Delete</button>
                            </form>
                        </div>
                      </td>';
                        echo '</tr>';
                    }
                } else {
                    echo '<tr><td colspan="5">No recommended bike models added yet.</td></tr>';
                }
                ?>
            </tbody>
        </table>
    </div>
    <script type="text/javascript">
        function confirmDelete(categoryName) {
            return confirm('Are you sure you want to delete the category "' + categoryName + '" and all its bike models?');
        }
    </script>
<?php
}

//recommend bike add  page
function recommend_bike_model_add_page_callback()
{
?>
    <div class="wrap">
        <h1>Add New Recommended Motor Model</h1>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
            <input type="hidden" name="action" value="add_new_recommend_bike_model">

            <label>Category Name:</label>
            <input type="text" name="category_name" required style="margin-bottom: 10px;"><br>

            <div id="bike-model-container">
                <div class="bike-model-entry">
                    <label>Motor Model:</label>
                    <select name="bike_model_id[]" class="bike-model-select" required onchange="updateDisabledOptions(this)">
                        <option value="" disabled selected>Select Motor Model</option>
                        <?php
                        $listing_posts = get_posts(array(
                            'post_type' => 'motorcycle-listing',
                            'posts_per_page' => -1
                        ));
                        $listings_by_make = [];
                        foreach ($listing_posts as $post) {
                            $makes = wp_get_post_terms($post->ID, 'make');
                            foreach ($makes as $make) {
                                $listings_by_make[$make->name][] = $post;
                            }
                        }

                        foreach ($listings_by_make as $make_name => $listings) {
                            echo '<optgroup label="' . esc_attr($make_name) . '">';
                            foreach ($listings as $post) {
                                echo '<option value="' . esc_attr($post->ID) . '">' . esc_html($post->post_title) . '</option>';
                            }
                            echo '</optgroup>';
                        }
                        ?>
                    </select>
                    <!-- Sort Field -->
                    <label>Sort:</label>
                    <input type="number" name="sort[]" required style="width: 60px;" min="1" value="1"><br>

                    <label>Type:</label>
                    <input type="number" name="type[]" required style="width: 60px;" min="1" value="1"><br>

                    <button type="button" class="remove-bike-model">Delete</button><br><br>
                </div>
            </div>

            <button type="button" id="add-bike-model">+</button><br><br>

            <!-- Weight -->
            <label>Weight:</label>
            <input type="number" name="bike_model_weight" required style="margin-bottom: 10px;"><br>

            <!-- Status -->
            <label>Status:</label>
            <label><input type="radio" name="bike_model_status" value="1" checked> Enable</label>
            <label><input type="radio" name="bike_model_status" value="0"> Disabled</label><br><br>

            <!-- Submit -->
            <input type="submit" value="Save" class="button button-primary">
        </form>
    </div>
    <script>
        document.getElementById('add-bike-model').addEventListener('click', function() {
            // Clone the first bike model entry and append it to the container
            const container = document.getElementById('bike-model-container');
            const newEntry = container.firstElementChild.cloneNode(true);

            // Reset values for the new entry
            newEntry.querySelector('select[name="bike_model_id[]"]').value = '';
            newEntry.querySelector('input[name="sort[]"]').value = '';
            newEntry.querySelector('input[name="type[]"]').value = '';

            // Disable already selected options
            const selectedOptions = Array.from(document.querySelectorAll('select[name="bike_model_id[]"] option:checked'));
            const allOptions = newEntry.querySelector('select[name="bike_model_id[]"]').options;

            selectedOptions.forEach(selected => {
                for (let option of allOptions) {
                    if (option.value === selected.value) {
                        option.disabled = true; // Disable previously selected options
                    }
                }
            });

            // Append the new entry to the container
            container.appendChild(newEntry);

            // Add remove functionality
            addRemoveFunctionality(newEntry);
        });

        function addRemoveFunctionality(entry) {
            const removeButton = entry.querySelector('.remove-bike-model');
            removeButton.addEventListener('click', function() {
                const container = document.getElementById('bike-model-container');

                // Check the number of entries before removing
                if (container.childElementCount > 1) {
                    // Remove the entry and enable the previously selected option
                    const select = entry.querySelector('select[name="bike_model_id[]"]');
                    const selectedOption = select.options[select.selectedIndex];
                    selectedOption.disabled = false; // Enable the option before removing

                    entry.remove(); // Remove the entry
                } else {
                    alert("At least one bike model must be present.");
                }
            });
        }

        // Add initial remove functionality to the first entry
        addRemoveFunctionality(document.querySelector('.bike-model-entry'));

        function updateDisabledOptions(selectedDropdown) {
            var allSelects = document.querySelectorAll('.bike-model-select');
            var selectedValue = selectedDropdown.value;

            allSelects.forEach(function(select) {
                if (select !== selectedDropdown) {
                    Array.from(select.options).forEach(function(option) {
                        option.disabled = false;
                        if (option.value === selectedValue) {
                            option.disabled = true;
                        }
                    });
                }
            });
        }
    </script>
    <?php
}

//add new recommend bikes
function recommend_handle_add_motor_form_submission()
{
    if (isset($_POST['bike_model_id']) && is_array($_POST['bike_model_id'])) {
        $recommend_bike_models = get_option('recommended_bike_models', []);
        $category_name = sanitize_text_field($_POST['category_name']);
        $weight = intval($_POST['bike_model_weight']);
        $status = intval($_POST['bike_model_status']);
        $positions = $_POST['sort'] ?? [];
        $types = $_POST['type'] ?? [];

        // $category_key = sanitize_title($category_name);

        if (!isset($recommend_bike_models[$category_name])) {
            $recommend_bike_models[$category_name] = [
                'category' => $category_name,
                'weight' => $weight,
                'status' => $status,
                'bike_models' => [],
            ];
        } else {
            $recommend_bike_models[$category_name]['weight'] = $weight;
            $recommend_bike_models[$category_name]['status'] = $status;
        }

        foreach ($_POST['bike_model_id'] as $index => $bike_model_id) {
            $bike_model_id = intval($bike_model_id);

            // Store the position of each bike model
            $position = isset($positions[$index]) ? intval($positions[$index]) : 1;
            $type = isset($types[$index]) ? intval($types[$index]) : 1;

            if (!in_array($bike_model_id, array_column($recommend_bike_models[$category_name]['bike_models'], 'id'))) {
                $recommend_bike_models[$category_name]['bike_models'][] = [
                    'id' => $bike_model_id,
                    'sort' => $position,
                    'type' => $type,
                ];
            }

            // update_post_meta($bike_model_id, 'bike_model_category_' . $category_key, $category_name);
        }

        update_option('recommended_bike_models', $recommend_bike_models);
    }

    wp_redirect(admin_url('admin.php?page=recommend-bike-model'));
    exit;
}
add_action('admin_post_add_new_recommend_bike_model', 'recommend_handle_add_motor_form_submission');

//recommend bike edit and view page
function recommend_bike_model_view_edit_page_callback()
{
    if (isset($_GET['category'])) {
        $category_name = isset($_GET['category']) ? sanitize_text_field($_GET['category']) : '';
        $recommend_bike_models = maybe_unserialize(get_option('recommended_bike_models', []));
        $is_edit = isset($_GET['edit']);
        $selected_bike_models = [];
        $positions = [];
        $types = [];
        $weight = '';
        $status = '';

        if (!empty($recommend_bike_models)) {
            foreach ($recommend_bike_models as $category_key => $model_data) {
                if ($model_data['category'] === $category_name) {
                    $weight = $model_data['weight'] ?? '';
                    $status = $model_data['status'] ?? '';

                    // Fetch the bike models
                    $selected_bike_models = $model_data['bike_models'] ?? [];

                    // Extract positions
                    if (!empty($selected_bike_models)) {
                        foreach ($selected_bike_models as $bike_model) {
                            $positions[$bike_model['id']] = $bike_model['sort'];
                            $types[$bike_model['id']] = $bike_model['type'];
                        }
                    }
                }
            }
        }
    ?>
        <div class="wrap">
            <h1><?php echo esc_html($is_edit ? 'Edit' : 'View') . ' Motor Model'; ?></h1>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <input type="hidden" name="action" value="<?php echo esc_attr($is_edit ? 'update_recommend_bike_model' : ''); ?>">

                <label>Category Name:</label>
                <input type="text" name="category_name" value="<?php echo esc_attr($category_name); ?>" <?php echo $is_edit ? '' : 'readonly'; ?> required style="margin-bottom: 10px;"><br>

                <label>Motor Models:</label>
                <div id="bike-model-container">
                    <?php
                    if (!empty($selected_bike_models)) {
                        foreach ($selected_bike_models as $bike_model) {
                            $selected_model_id = $bike_model['id'];
                            $position_value = $positions[$selected_model_id] ?? 1;
                            $type_value = $types[$selected_model_id] ?? 1;
                    ?>
                            <div class="bike-model-entry" <?php echo $is_edit ? 'style="margin-bottom: -50px;"' : ''; ?>>
                                <select name="bike_model_id[]" class="bike-model-select" required <?php echo !$is_edit ? 'disabled' : ''; ?> onchange="updateDisabledOptions(this)">
                                    <option value="" disabled>Select Motor Model</option>
                                    <?php
                                    // Display the bike models in the select dropdown
                                    $listing_posts = get_posts(array(
                                        'post_type' => 'motorcycle-listing',
                                        'posts_per_page' => -1
                                    ));

                                    $listings_by_make = [];
                                    foreach ($listing_posts as $post) {
                                        $makes = wp_get_post_terms($post->ID, 'make');
                                        foreach ($makes as $make) {
                                            $listings_by_make[$make->name][] = $post;
                                        }
                                    }

                                    foreach ($listings_by_make as $make_name => $listings) {
                                        echo '<optgroup label="' . esc_attr($make_name) . '">';
                                        foreach ($listings as $post) {
                                            $selected = ($post->ID == $selected_model_id) ? 'selected' : '';
                                            echo '<option value="' . esc_attr($post->ID) . '" ' . $selected . '>' . esc_html($post->post_title) . '</option>';
                                        }
                                        echo '</optgroup>';
                                    }
                                    ?>
                                </select>
                                <!-- Sort Field -->
                                <label>Sort:</label>
                                <input type="number" name="sort[]" required <?php echo !$is_edit ? 'disabled' : ''; ?> style="width: 60px;" min="1" value="<?php echo esc_attr($position_value); ?>"><br>

                                <label>Type:</label>
                                <input type="number" name="type[]" required <?php echo !$is_edit ? 'disabled' : ''; ?> style="width: 60px;" min="1" value="<?php echo esc_attr($type_value); ?>"><br>

                                <?php if ($is_edit): ?>
                                    <button type="button" class="remove-bike-model">Delete</button><br><br>
                                <?php endif; ?>
                                <br><br>
                            </div>
                        <?php
                        }
                    } else {
                        ?>
                        <div class="bike-model-entry">
                            <select name="bike_model_id[]" class="bike-model-select" required onchange="updateDisabledOptions(this)">
                                <option value="" disabled selected>Select Motor Model</option>
                                <?php
                                $listing_posts = get_posts(array(
                                    'post_type' => 'listing',
                                    'posts_per_page' => -1
                                ));

                                $listings_by_make = [];
                                foreach ($listing_posts as $post) {
                                    $makes = wp_get_post_terms($post->ID, 'make');
                                    foreach ($makes as $make) {
                                        $listings_by_make[$make->name][] = $post;
                                    }
                                }

                                foreach ($listings_by_make as $make_name => $listings) {
                                    echo '<optgroup label="' . esc_attr($make_name) . '">';
                                    foreach ($listings as $post) {
                                        echo '<option value="' . esc_attr($post->ID) . '">' . esc_html($post->post_title) . '</option>';
                                    }
                                    echo '</optgroup>';
                                }
                                ?>
                            </select>
                            <!-- Position Field -->
                            <label>Position:</label>
                            <input type="number" name="sort[]" required style="width: 60px;" min="1" value="1"><br>

                            <label>Position:</label>
                            <input type="number" name="type[]" required style="width: 60px;" min="1" value="1"><br>

                            <button type="button" class="remove-bike-model">Delete</button><br><br>
                        </div>
                    <?php
                    }
                    ?>
                </div>

                <?php if ($is_edit): ?>
                    <button type="button" id="add-bike-model">+</button><br><br>
                <?php endif; ?>

                <label>Weight:</label>
                <input type="number" name="bike_model_weight" value="<?php echo esc_attr($weight); ?>" <?php echo $is_edit ? '' : 'readonly'; ?> required style="margin-bottom: 10px;"><br>

                <label>Status:</label>
                <label><input type="radio" name="bike_model_status" value="1" <?php checked($status, 1); ?> <?php echo $is_edit ? '' : 'disabled'; ?>> Enable</label>
                <label><input type="radio" name="bike_model_status" value="0" <?php checked($status, 0); ?> <?php echo $is_edit ? '' : 'disabled'; ?>> Disabled</label><br><br>

                <?php if ($is_edit) : ?>
                    <input type="submit" style="margin-bottom: 10px;" value="Update" class="button button-primary">
                <?php endif; ?>
            </form>
            <a href="<?php echo esc_url(admin_url('admin.php?page=recommend-bike-model')); ?>" class="button">Back to List</a>
        </div>

        <script>
            document.getElementById('add-bike-model').addEventListener('click', function() {
                const container = document.getElementById('bike-model-container');
                const newEntry = container.firstElementChild.cloneNode(true);
                newEntry.querySelector('select[name="bike_model_id[]"]').value = '';
                newEntry.querySelector('input[name="sort[]"]').value = '';
                newEntry.querySelector('input[name="type[]"]').value = '';

                const selectedOptions = Array.from(document.querySelectorAll('select[name="bike_model_id[]"] option:checked'));
                const allOptions = newEntry.querySelector('select[name="bike_model_id[]"]').options;

                selectedOptions.forEach(selected => {
                    for (let option of allOptions) {
                        if (option.value === selected.value) {
                            option.disabled = true;
                        }
                    }
                });

                container.appendChild(newEntry);

                addRemoveFunctionality(newEntry);
            });

            // Function to add delete functionality to each bike model entry
            function addRemoveFunctionality(entry) {
                const removeButton = entry.querySelector('.remove-bike-model');
                removeButton.addEventListener('click', function() {
                    const container = document.getElementById('bike-model-container');

                    if (container.childElementCount > 1) {
                        const select = entry.querySelector('select[name="bike_model_id[]"]');
                        console.log('delete data', select.selectedIndex);
                        const selectedOption = select.options[select.selectedIndex];
                        selectedOption.disabled = false;

                        entry.remove();
                    } else {
                        alert("At least one bike model must be present.");
                    }
                });
            }

            document.querySelectorAll('.bike-model-entry').forEach(function(entry) {
                addRemoveFunctionality(entry);
            });

            function updateDisabledOptions(selectedDropdown) {
                var allSelects = document.querySelectorAll('.bike-model-select');
                var selectedValue = selectedDropdown.value;

                allSelects.forEach(function(select) {
                    if (select !== selectedDropdown) {
                        var options = select.options;
                        for (var i = 0; i < options.length; i++) {
                            if (options[i].value === selectedValue) {
                                options[i].disabled = true;
                            } else {
                                options[i].disabled = false;
                            }
                        }
                    }
                });
            }
        </script>
    <?php
    }
}

// Update recommend bikes
function recommend_handle_update_motor_form_submission()
{
    if (isset($_POST['bike_model_id']) && is_array($_POST['bike_model_id'])) {
        $recommend_bike_models = get_option('recommended_bike_models', []);
        $category_name = sanitize_text_field($_POST['category_name']);
        $weight = sanitize_text_field($_POST['bike_model_weight']);
        $status = isset($_POST['bike_model_status']) ? intval($_POST['bike_model_status']) : 0;

        $positions = array_map('sanitize_text_field', $_POST['sort']);
        $types = array_map('sanitize_text_field', $_POST['type']);

        // $normalized_category_name = strtolower(str_replace(' ', '-', $category_name));

        if (array_key_exists($category_name, $recommend_bike_models)) {
            $recommend_bike_models[$category_name]['weight'] = $weight;
            $recommend_bike_models[$category_name]['status'] = $status;

            $bike_models = [];
            foreach ($_POST['bike_model_id'] as $index => $model_id) {
                $bike_models[] = [
                    'id' => intval($model_id),
                    'sort' => intval($positions[$index]),
                    'type' => intval($types[$index])
                ];
            }
            $recommend_bike_models[$category_name]['bike_models'] = $bike_models;
        } else {

            $recommend_bike_models[$category_name] = [
                'category' => $category_name,
                'weight' => $weight,
                'status' => $status,
                'bike_models' => []
            ];

            foreach ($_POST['bike_model_id'] as $index => $model_id) {
                $recommend_bike_models[$category_name]['bike_models'][] = [
                    'id' => intval($model_id),
                    'sort' => intval($positions[$index]),
                    'type' => intval($types[$index])
                ];
            }
        }

        update_option('recommended_bike_models', $recommend_bike_models);

        wp_redirect(admin_url('admin.php?page=recommend-bike-model'));
        exit;
    } else {
        error_log('No bike_model_id found in the POST request.');
    }
}

add_action('admin_post_update_recommend_bike_model', 'recommend_handle_update_motor_form_submission');


//delete recommend bikes
function recommend_handle_delete_motor_form_submission()
{
    if (isset($_POST['category_name'])) {
        $category_name = sanitize_text_field($_POST['category_name']);
        $recommend_bike_models = get_option('recommended_bike_models', []);

        // $normalized_category_key = strtolower(str_replace(' ', '-', $category_name));

        if (isset($recommend_bike_models[$category_name])) {
            unset($recommend_bike_models[$category_name]);
        }

        update_option('recommended_bike_models', $recommend_bike_models);
    }

    wp_redirect(admin_url('admin.php?page=recommend-bike-model'));
    exit;
}

add_action('admin_post_delete_recommend_bike_model', 'recommend_handle_delete_motor_form_submission');


function recommend_bike_model_admin_menu()
{
    add_menu_page('Recommended bike Models', 'Recommended bike Models', 'manage_options', 'recommend-bike-model', 'recommend_bike_model_page_callback');
    add_submenu_page('recommend-bike-model', 'Add New bike Model', 'Add New bike Model', 'manage_options', 'add-new-recommend-bike', 'recommend_bike_model_add_page_callback');
    add_submenu_page(null, 'View/Edit bike Model', 'View/Edit bike Model', 'manage_options', 'view-edit-recommend-bike', 'recommend_bike_model_view_edit_page_callback');
}
add_action('admin_menu', 'recommend_bike_model_admin_menu');


//top bike model page data
function top_bike_model_page_callback()
{
    $top_bike_models = get_option('top_bike_models', []);
    $grouped_models = [];

    if (!empty($top_bike_models)) {
        foreach ($top_bike_models as $category_key => $data) {
            if (isset($data['type'], $data['bike_models'])) {
                $type = $data['type'];

                if (!isset($grouped_models[$type])) {
                    $grouped_models[$type] = [
                        'models' => [],
                    ];
                }

                foreach ($data['bike_models'] as $bike_model_data) {
                    $bike_model_id = isset($bike_model_data['id']) ? $bike_model_data['id'] : null;
                    if ($bike_model_id) {
                        $bike_model = get_post($bike_model_id);
                        if ($bike_model) {
                            // Append only the bike model title
                            $grouped_models[$type]['models'][] = $bike_model->post_title;
                        }
                    }
                }
            }
        }
    }
    ?>
    <div class="wrap">
        <h1>Top Motor Models</h1>
        <a href="<?php echo admin_url('admin.php?page=add-new-top-bike'); ?>" class="button button-primary" style="margin-bottom: 20px;">Add New Motor Model</a>
        <table class="wp-list-table widefat fixed striped">
            <thead>
                <tr>
                    <th>Type</th>
                    <th>Motor Models</th>
                    <th>Operate</th>
                </tr>
            </thead>
            <tbody>
                <?php
                if (!empty($grouped_models)) {
                    foreach ($grouped_models as $type => $data) {
                        $bike_models_list = implode(', ', $data['models']);
                        echo '<tr>';
                        echo '<td>' . esc_html($type) . '</td>';
                        echo '<td>' . esc_html($bike_models_list) . '</td>';
                        echo '<td>
                        <div style="display: inline-block; margin-right: 5px;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-top-bike&type=' . $type)) . '" class="button">View</a>
                        </div>
                        <div style="display: inline-block;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-top-bike&type=' . $type . '&edit=true')) . '" class="button">Edit</a>
                        </div>
                        <div style="display: inline-block; margin-right: 5px;">
                            <form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" onsubmit="return confirmDelete(\'' . esc_js($type) . '\');">
                                <input type="hidden" name="action" value="delete_top_bike_model">
                                <input type="hidden" name="type" value="' . esc_attr($type) . '">
                                <button type="submit" class="button button-secondary">Delete</button>
                            </form>
                        </div>
                      </td>';
                        echo '</tr>';
                    }
                } else {
                    echo '<tr><td colspan="5">No top bike models added yet.</td></tr>';
                }
                ?>
            </tbody>
        </table>
    </div>
    <script type="text/javascript">
        function confirmDelete(type) {
            return confirm('Are you sure you want to delete the category "' + type + '" and all its bike models?');
        }
    </script>
<?php
}

//recommend bike add  page
function top_bike_model_add_page_callback()
{
?>
    <div class="wrap">
        <h1>Add New Top Motor Model</h1>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
            <input type="hidden" name="action" value="add_new_top_bike_model">

            <label>Type:</label>
            <input type="text" name="type" required style="margin-bottom: 10px;"><br>

            <div id="bike-model-container">
                <div class="bike-model-entry">
                    <label>Motor Model:</label>
                    <select name="bike_model_id[]" class="bike-model-select" required onchange="updateDisabledOptions(this)">
                        <option value="" disabled selected>Select Motor Model</option>
                        <?php
                        $listing_posts = get_posts(array(
                            'post_type' => 'listing',
                            'posts_per_page' => -1
                        ));
                        $listings_by_make = [];
                        foreach ($listing_posts as $post) {
                            $makes = wp_get_post_terms($post->ID, 'listing_make');
                            foreach ($makes as $make) {
                                $listings_by_make[$make->name][] = $post;
                            }
                        }

                        foreach ($listings_by_make as $make_name => $listings) {
                            echo '<optgroup label="' . esc_attr($make_name) . '">';
                            foreach ($listings as $post) {
                                echo '<option value="' . esc_attr($post->ID) . '">' . esc_html($post->post_title) . '</option>';
                            }
                            echo '</optgroup>';
                        }
                        ?>
                    </select>
                    <!-- Position Field -->
                    <label>Position:</label>
                    <input type="number" name="position[]" required style="width: 60px;" min="1" value="1"><br>

                    <button type="button" class="remove-bike-model">Delete</button><br><br>
                </div>
            </div>

            <button type="button" id="add-bike-model">+</button><br><br>

            <!-- Submit -->
            <input type="submit" value="Save" class="button button-primary">
        </form>
    </div>
    <script>
        document.getElementById('add-bike-model').addEventListener('click', function() {
            const container = document.getElementById('bike-model-container');
            const newEntry = container.firstElementChild.cloneNode(true);

            newEntry.querySelector('select[name="bike_model_id[]"]').value = '';
            newEntry.querySelector('input[name="position[]"]').value = '';

            const selectedOptions = Array.from(document.querySelectorAll('select[name="bike_model_id[]"] option:checked'));
            const allOptions = newEntry.querySelector('select[name="bike_model_id[]"]').options;

            selectedOptions.forEach(selected => {
                for (let option of allOptions) {
                    if (option.value === selected.value) {
                        option.disabled = true;
                    }
                }
            });

            container.appendChild(newEntry);

            addRemoveFunctionality(newEntry);
        });

        function addRemoveFunctionality(entry) {
            const removeButton = entry.querySelector('.remove-bike-model');
            removeButton.addEventListener('click', function() {
                const container = document.getElementById('bike-model-container');

                if (container.childElementCount > 1) {
                    const select = entry.querySelector('select[name="bike_model_id[]"]');
                    const selectedOption = select.options[select.selectedIndex];
                    selectedOption.disabled = false;

                    entry.remove();
                } else {
                    alert("At least one bike model must be present.");
                }
            });
        }

        // Add initial remove functionality to the first entry
        addRemoveFunctionality(document.querySelector('.bike-model-entry'));

        function updateDisabledOptions(selectedDropdown) {
            var allSelects = document.querySelectorAll('.bike-model-select');
            var selectedValue = selectedDropdown.value;

            allSelects.forEach(function(select) {
                if (select !== selectedDropdown) {
                    Array.from(select.options).forEach(function(option) {
                        option.disabled = false;
                        if (option.value === selectedValue) {
                            option.disabled = true;
                        }
                    });
                }
            });
        }
    </script>
    <?php
}

//add new top bikes
function top_handle_add_motor_form_submission()
{
    if (isset($_POST['bike_model_id']) && is_array($_POST['bike_model_id'])) {
        $top_bike_models = get_option('top_bike_models', []);
        $type = sanitize_text_field($_POST['type']);
        $positions = $_POST['position'] ?? [];

        $type_key = sanitize_title($type);

        if (!isset($top_bike_models[$type_key])) {
            $top_bike_models[$type_key] = [
                'type' => $type,
                'bike_models' => [],
            ];
        }

        foreach ($_POST['bike_model_id'] as $index => $bike_model_id) {
            $bike_model_id = intval($bike_model_id);

            // Store the position of each bike model
            $position = isset($positions[$index]) ? intval($positions[$index]) : 1;

            if (!in_array($bike_model_id, array_column($top_bike_models[$type_key]['bike_models'], 'id'))) {
                $top_bike_models[$type_key]['bike_models'][] = [
                    'id' => $bike_model_id,
                    'position' => $position,
                ];
            }
        }

        update_option('top_bike_models', $top_bike_models);
    }

    wp_redirect(admin_url('admin.php?page=top-bike-model'));
    exit;
}
add_action('admin_post_add_new_top_bike_model', 'top_handle_add_motor_form_submission');

//top bike edit and view page
function top_bike_model_view_edit_page_callback()
{
    if (isset($_GET['type'])) {
        $type = isset($_GET['type']) ? sanitize_text_field($_GET['type']) : '';
        $top_bike_models = maybe_unserialize(get_option('top_bike_models', []));
        $is_edit = isset($_GET['edit']);
        $selected_bike_models = [];
        $positions = [];

        if (!empty($top_bike_models)) {
            foreach ($top_bike_models as $key => $model_data) {
                if ($model_data['type'] === $type) {
                    $selected_bike_models = $model_data['bike_models'] ?? [];

                    // Extract positions
                    if (!empty($selected_bike_models)) {
                        foreach ($selected_bike_models as $bike_model) {
                            $positions[$bike_model['id']] = $bike_model['position'];
                        }
                    }
                }
            }
        }
    ?>
        <div class="wrap">
            <h1><?php echo esc_html($is_edit ? 'Edit' : 'View') . ' Motor Model'; ?></h1>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <input type="hidden" name="action" value="<?php echo esc_attr($is_edit ? 'update_top_bike_model' : ''); ?>">

                <label>Type:</label>
                <input type="text" name="type" value="<?php echo esc_attr($type); ?>" <?php echo $is_edit ? '' : 'readonly'; ?> required style="margin-bottom: 10px;"><br>

                <label>Motor Models:</label>
                <div id="bike-model-container">
                    <?php
                    if (!empty($selected_bike_models)) {
                        foreach ($selected_bike_models as $bike_model) {
                            $selected_model_id = $bike_model['id'];
                            $position_value = $positions[$selected_model_id] ?? 1;
                    ?>
                            <div class="bike-model-entry" <?php echo $is_edit ? 'style="margin-bottom: -50px;"' : ''; ?>>
                                <select name="bike_model_id[]" class="bike-model-select" required <?php echo !$is_edit ? 'disabled' : ''; ?> onchange="updateDisabledOptions(this)">
                                    <option value="" disabled>Select Motor Model</option>
                                    <?php
                                    // Display the bike models in the select dropdown
                                    $listing_posts = get_posts(array(
                                        'post_type' => 'listing',
                                        'posts_per_page' => -1
                                    ));

                                    $listings_by_make = [];
                                    foreach ($listing_posts as $post) {
                                        $makes = wp_get_post_terms($post->ID, 'listing_make');
                                        foreach ($makes as $make) {
                                            $listings_by_make[$make->name][] = $post;
                                        }
                                    }

                                    foreach ($listings_by_make as $make_name => $listings) {
                                        echo '<optgroup label="' . esc_attr($make_name) . '">';
                                        foreach ($listings as $post) {
                                            $selected = ($post->ID == $selected_model_id) ? 'selected' : '';
                                            echo '<option value="' . esc_attr($post->ID) . '" ' . $selected . '>' . esc_html($post->post_title) . '</option>';
                                        }
                                        echo '</optgroup>';
                                    }
                                    ?>
                                </select>
                                <!-- Position Field -->
                                <label>Position:</label>
                                <input type="number" name="position[]" required <?php echo !$is_edit ? 'disabled' : ''; ?> style="width: 60px;" min="1" value="<?php echo esc_attr($position_value); ?>"><br>

                                <?php if ($is_edit): ?>
                                    <button type="button" class="remove-bike-model">Delete</button><br><br>
                                <?php endif; ?>
                                <br><br>
                            </div>
                        <?php
                        }
                    } else {
                        ?>
                        <div class="bike-model-entry">
                            <select name="bike_model_id[]" class="bike-model-select" required onchange="updateDisabledOptions(this)">
                                <option value="" disabled selected>Select Motor Model</option>
                                <?php
                                $listing_posts = get_posts(array(
                                    'post_type' => 'listing',
                                    'posts_per_page' => -1
                                ));

                                $listings_by_make = [];
                                foreach ($listing_posts as $post) {
                                    $makes = wp_get_post_terms($post->ID, 'listing_make');
                                    foreach ($makes as $make) {
                                        $listings_by_make[$make->name][] = $post;
                                    }
                                }

                                foreach ($listings_by_make as $make_name => $listings) {
                                    echo '<optgroup label="' . esc_attr($make_name) . '">';
                                    foreach ($listings as $post) {
                                        echo '<option value="' . esc_attr($post->ID) . '">' . esc_html($post->post_title) . '</option>';
                                    }
                                    echo '</optgroup>';
                                }
                                ?>
                            </select>
                            <!-- Position Field -->
                            <label>Position:</label>
                            <input type="number" name="position[]" required style="width: 60px;" min="1" value="1"><br>

                            <button type="button" class="remove-bike-model">Delete</button><br><br>
                        </div>
                    <?php
                    }
                    ?>
                </div>

                <?php if ($is_edit): ?>
                    <button type="button" id="add-bike-model">+</button><br><br>
                <?php endif; ?>

                <?php if ($is_edit) : ?>
                    <input type="submit" style="margin-bottom: 10px;" value="Update" class="button button-primary">
                <?php endif; ?>
            </form>
            <a href="<?php echo esc_url(admin_url('admin.php?page=top-bike-model')); ?>" class="button">Back to List</a>
        </div>

        <script>
            document.getElementById('add-bike-model').addEventListener('click', function() {
                const container = document.getElementById('bike-model-container');
                const newEntry = container.firstElementChild.cloneNode(true);
                newEntry.querySelector('select[name="bike_model_id[]"]').value = '';
                newEntry.querySelector('input[name="position[]"]').value = '';

                const selectedOptions = Array.from(document.querySelectorAll('select[name="bike_model_id[]"] option:checked'));
                const allOptions = newEntry.querySelector('select[name="bike_model_id[]"]').options;

                selectedOptions.forEach(selected => {
                    for (let option of allOptions) {
                        if (option.value === selected.value) {
                            option.disabled = true;
                        }
                    }
                });

                container.appendChild(newEntry);

                addRemoveFunctionality(newEntry);
            });

            // Function to add delete functionality to each bike model entry
            function addRemoveFunctionality(entry) {
                const removeButton = entry.querySelector('.remove-bike-model');
                removeButton.addEventListener('click', function() {
                    const container = document.getElementById('bike-model-container');

                    if (container.childElementCount > 1) {
                        const select = entry.querySelector('select[name="bike_model_id[]"]');
                        console.log('delete data', select.selectedIndex);
                        const selectedOption = select.options[select.selectedIndex];
                        selectedOption.disabled = false;

                        entry.remove();
                    } else {
                        alert("At least one bike model must be present.");
                    }
                });
            }

            document.querySelectorAll('.bike-model-entry').forEach(function(entry) {
                addRemoveFunctionality(entry);
            });

            function updateDisabledOptions(selectedDropdown) {
                var allSelects = document.querySelectorAll('.bike-model-select');
                var selectedValue = selectedDropdown.value;

                allSelects.forEach(function(select) {
                    if (select !== selectedDropdown) {
                        var options = select.options;
                        for (var i = 0; i < options.length; i++) {
                            if (options[i].value === selectedValue) {
                                options[i].disabled = true;
                            } else {
                                options[i].disabled = false;
                            }
                        }
                    }
                });
            }
        </script>
<?php
    }
}

// Update top bikes
function top_handle_update_motor_form_submission()
{
    if (isset($_POST['bike_model_id']) && is_array($_POST['bike_model_id'])) {
        $top_bike_models = get_option('top_bike_models', []);
        $type = sanitize_text_field($_POST['type']);

        $positions = array_map('sanitize_text_field', $_POST['position']);

        $normalized_type = strtolower(str_replace(' ', '-', $type));

        if (array_key_exists($normalized_type, $top_bike_models)) {
            $bike_models = [];
            foreach ($_POST['bike_model_id'] as $index => $model_id) {
                $bike_models[] = [
                    'id' => intval($model_id),
                    'position' => intval($positions[$index])
                ];
            }
            $top_bike_models[$normalized_type]['bike_models'] = $bike_models;
        } else {

            $top_bike_models[$normalized_type] = [
                'type' => $type,
                'bike_models' => []
            ];

            foreach ($_POST['bike_model_id'] as $index => $model_id) {
                $top_bike_models[$normalized_type]['bike_models'][] = [
                    'id' => intval($model_id),
                    'position' => intval($positions[$index])
                ];
            }
        }

        update_option('top_bike_models', $top_bike_models);

        wp_redirect(admin_url('admin.php?page=top-bike-model'));
        exit;
    } else {
        error_log('No bike_model_id found in the POST request.');
    }
}

add_action('admin_post_update_top_bike_model', 'top_handle_update_motor_form_submission');


//delete top bikes
function top_handle_delete_motor_form_submission()
{
    if (isset($_POST['type'])) {
        $type = sanitize_text_field($_POST['type']);
        $top_bike_models = get_option('top_bike_models', []);

        $normalized_category_key = strtolower(str_replace(' ', '-', $type));

        if (isset($top_bike_models[$normalized_category_key])) {
            unset($top_bike_models[$normalized_category_key]);
        }

        update_option('top_bike_models', $top_bike_models);
    }

    wp_redirect(admin_url('admin.php?page=top-bike-model'));
    exit;
}

add_action('admin_post_delete_top_bike_model', 'top_handle_delete_motor_form_submission');

function top_bike_model_admin_menu()
{
    add_menu_page('Top bike Models', 'Top bike Models', 'manage_options', 'top-bike-model', 'top_bike_model_page_callback');
    add_submenu_page('top-bike-model', 'Add New bike Model', 'Add New bike Model', 'manage_options', 'add-new-top-bike', 'top_bike_model_add_page_callback');
    add_submenu_page(null, 'View/Edit Motor Model', 'View/Edit Motor Model', 'manage_options', 'view-edit-top-bike', 'top_bike_model_view_edit_page_callback');
}
add_action('admin_menu', 'top_bike_model_admin_menu');
