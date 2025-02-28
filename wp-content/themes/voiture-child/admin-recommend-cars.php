<?php
function custom_admin_menu()
{
    // Add the main menu page
    add_menu_page(
        'Recommended',
        'Recommended',
        'manage_options',
        'custom_menu',
        'menu_page_callback',
        'dashicons-menu',
        6
    );

    // Add the 'Top Car Model' submenu page
    add_submenu_page(
        'custom_menu',
        'Top Car Model',
        'Top Car Model',
        'manage_options',
        'top_car_model', // Menu slug
        'top_car_model_page_callback'
    );

    // Add the 'Recommend Car Model' submenu page
    add_submenu_page(
        'custom_menu',
        'Recommend Car Model',
        'Recommend Car Model',
        'manage_options',
        'recommend_car_model',
        'recommend_car_model_page_callback' // Callback function
    );

    // Add the 'Ev Car Model' submenu page
    add_submenu_page(
        'custom_menu',
        'EV Car Model',
        'EV Car Model',
        'manage_options',
        'ev_car_model',
        'ev_car_model_page_callback' // Callback function
    );
}
add_action('admin_menu', 'custom_admin_menu');

//recommend car listing page
function recommend_car_model_page_callback()
{
    $recommend_car_models = get_option('recommended_car_models', []);
    $grouped_models = [];

    if (!empty($recommend_car_models)) {
        foreach ($recommend_car_models as $category_key => $data) {
            if (isset($data['category'], $data['car_models'])) {
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

                foreach ($data['car_models'] as $car_model_data) {
                    $car_model_id = isset($car_model_data['id']) ? $car_model_data['id'] : null;
                    if ($car_model_id) {
                        $car_model = get_post($car_model_id);
                        if ($car_model) {
                            $grouped_models[$category_name]['models'][] = $car_model->post_title;
                        }
                    }
                }
            }
        }
    }
?>
    <div class="wrap">
        <h1>Recommended Car Models</h1>
        <a href="<?php echo admin_url('admin.php?page=add-new-recommend-car'); ?>" class="button button-primary" style="margin-bottom: 20px;">Add New Car Model</a>
        <table class="wp-list-table widefat fixed striped">
            <thead>
                <tr>
                    <th>Category Name</th>
                    <th>Car Models</th>
                    <th>Weight</th>
                    <th>Status</th>
                    <th>Operate</th>
                </tr>
            </thead>
            <tbody>
                <?php
                if (!empty($grouped_models)) {
                    foreach ($grouped_models as $category_name => $data) {
                        $car_models_list = implode(', ', $data['models']);
                        echo '<tr>';
                        echo '<td>' . esc_html($category_name) . '</td>';
                        echo '<td>' . esc_html($car_models_list) . '</td>';
                        echo '<td>' . esc_html($data['weight']) . '</td>';
                        echo '<td>' . esc_html($data['status']) . '</td>';
                        echo '<td>
                        <div style="display: inline-block; margin-right: 5px;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-recommend-car&category=' . $category_name)) . '" class="button">View</a>
                        </div>
                        <div style="display: inline-block;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-recommend-car&category=' . $category_name . '&edit=true')) . '" class="button">Edit</a>
                        </div>
                        <div style="display: inline-block; margin-right: 5px;">
                            <form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" onsubmit="return confirmDelete(\'' . esc_js($category_name) . '\');">
                                <input type="hidden" name="action" value="delete_recommend_car_model">
                                <input type="hidden" name="category_name" value="' . esc_attr($category_name) . '">
                                <button type="submit" class="button button-secondary">Delete</button>
                            </form>
                        </div>
                      </td>';
                        echo '</tr>';
                    }
                } else {
                    echo '<tr><td colspan="5">No recommended car models added yet.</td></tr>';
                }
                ?>
            </tbody>
        </table>
    </div>
    <script type="text/javascript">
        function confirmDelete(categoryName) {
            return confirm('Are you sure you want to delete the category "' + categoryName + '" and all its car models?');
        }
    </script>
<?php
}

//recommend car add  page
function recommend_car_model_add_page_callback()
{
?>
    <div class="wrap">
        <h1>Add New Recommended Car Model</h1>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">

            <input type="hidden" name="action" value="add_new_recommend_car_model">

            <label>Category Name:</label>
            <input type="text" name="category_name" required style="margin-bottom: 10px; margin-bottom: 10px;margin-left: 20px;width: 271px;"><br>

            <div id="car-model-container">

                <div class="car-model-entry" style="display:flex; align-items:center;">
                    <label>Car Model:</label>
                    <select name="car_model_id[]" style="margin-left: 44px;" class="car-model-select" required onchange="updateDisabledOptions(this)">
                        <option value="" disabled selected>Select Car Model</option>
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
                    <!-- Sort Field -->
                    <label style=" margin-left: 10px;">Sort:</label>
                    <input type="number" name="sort[]" required style="width: 60px;" min="0" value="0"><br>

                    <label style=" margin-left: 10px;">Type:</label>
                    <input type="number" name="type[]" required style="width: 60px;" min="1" value="1"><br>

                    <button type="button" style=" width: 70px; height: 30px; border-radius: 3px; border: 1px solid #9d9d9d; margin-left: 20px;" class="remove-car-model">Delete</button><br><br>
                </div>
            </div><br>


            <button type="button" style="width: 70px; height: 30px; margin-bottom: 10px; background: #135E96; color: #ffffff; border: none; border-radius: 3px;" id="add-car-model">Add</button><br><br>
            <!-- Weight -->
            <label>Weight:</label>
            <input type="number" name="car_model_weight" required style="margin-bottom: 10px; margin-bottom: 10px; margin-left: 69px; width: 272px;"><br>

            <!-- Status -->
            <label>Status:</label>
            <label><input type="radio" name="car_model_status" value="1" checked> Enable</label>
            <label><input type="radio" name="car_model_status" value="0"> Disabled</label><br><br>

            <!-- Submit -->
            <input type="submit" value="Save Changes" class="button button-primary">
        </form>
    </div>
    <style>
        .car-model-entry {
            display: flex !important;
            gap: 10px;
        }
    </style>
    <script>
        document.getElementById('add-car-model').addEventListener('click', function() {
            // Clone the first car model entry and append it to the container
            const container = document.getElementById('car-model-container');
            const newEntry = container.firstElementChild.cloneNode(true);

            // Reset values for the new entry
            newEntry.querySelector('select[name="car_model_id[]"]').value = '';
            newEntry.querySelector('input[name="sort[]"]').value = '';
            newEntry.querySelector('input[name="type[]"]').value = '';

            // Disable already selected options
            const selectedOptions = Array.from(document.querySelectorAll('select[name="car_model_id[]"] option:checked'));
            const allOptions = newEntry.querySelector('select[name="car_model_id[]"]').options;

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
            const removeButton = entry.querySelector('.remove-car-model');
            removeButton.addEventListener('click', function() {
                const container = document.getElementById('car-model-container');

                // Check the number of entries before removing
                if (container.childElementCount > 1) {
                    // Remove the entry and enable the previously selected option
                    const select = entry.querySelector('select[name="car_model_id[]"]');
                    const selectedOption = select.options[select.selectedIndex];
                    selectedOption.disabled = false; // Enable the option before removing

                    entry.remove(); // Remove the entry
                } else {
                    alert("At least one car model must be present.");
                }
            });
        }

        // Add initial remove functionality to the first entry
        addRemoveFunctionality(document.querySelector('.car-model-entry'));

        function updateDisabledOptions(selectedDropdown) {
            var allSelects = document.querySelectorAll('.car-model-select');
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

//add new recommend cars
function recommend_handle_add_form_submission()
{
    if (isset($_POST['car_model_id']) && is_array($_POST['car_model_id'])) {
        $recommend_car_models = get_option('recommended_car_models', []);
        $category_name = sanitize_text_field($_POST['category_name']);
        $weight = intval($_POST['car_model_weight']);
        $status = intval($_POST['car_model_status']);
        $positions = $_POST['sort'] ?? [];
        $types = $_POST['type'] ?? [];

        // $category_key = sanitize_title($category_name);

        if (!isset($recommend_car_models[$category_name])) {
            $recommend_car_models[$category_name] = [
                'category' => $category_name,
                'weight' => $weight,
                'status' => $status,
                'car_models' => [],
            ];
        } else {
            $recommend_car_models[$category_name]['weight'] = $weight;
            $recommend_car_models[$category_name]['status'] = $status;
        }

        foreach ($_POST['car_model_id'] as $index => $car_model_id) {
            $car_model_id = intval($car_model_id);

            // Store the position of each car model
            $position = isset($positions[$index]) ? intval($positions[$index]) : 1;
            $type = isset($types[$index]) ? intval($types[$index]) : 1;

            if (!in_array($car_model_id, array_column($recommend_car_models[$category_name]['car_models'], 'id'))) {
                $recommend_car_models[$category_name]['car_models'][] = [
                    'id' => $car_model_id,
                    'sort' => $position,
                    'type' => $type,
                ];
            }

            // update_post_meta($car_model_id, 'car_model_category_' . $category_key, $category_name);
        }

        update_option('recommended_car_models', $recommend_car_models);
    }

    wp_redirect(admin_url('admin.php?page=recommend-car-model'));
    exit;
}
add_action('admin_post_add_new_recommend_car_model', 'recommend_handle_add_form_submission');

//recommend car edit and view page
function recommend_car_model_view_edit_page_callback()
{
    if (isset($_GET['category'])) {
        $category_name = isset($_GET['category']) ? sanitize_text_field($_GET['category']) : '';
        $recommend_car_models = maybe_unserialize(get_option('recommended_car_models', []));
        $is_edit = isset($_GET['edit']);
        $selected_car_models = [];
        $positions = [];
        $types = [];
        $weight = '';
        $status = '';

        if (!empty($recommend_car_models)) {
            foreach ($recommend_car_models as $category_key => $model_data) {
                if ($model_data['category'] === $category_name) {
                    $weight = $model_data['weight'] ?? '';
                    $status = $model_data['status'] ?? '';

                    // Fetch the car models
                    $selected_car_models = $model_data['car_models'] ?? [];

                    // Extract positions
                    if (!empty($selected_car_models)) {
                        foreach ($selected_car_models as $car_model) {
                            $positions[$car_model['id']] = $car_model['sort'];
                            $types[$car_model['id']] = $car_model['type'];
                        }
                    }
                }
            }
        }
    ?>
        <div class="wrap">
            <h1><?php echo esc_html($is_edit ? 'Edit' : 'View') . ' Car Model'; ?></h1>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <input type="hidden" name="action" value="<?php echo esc_attr($is_edit ? 'update_recommend_car_model' : ''); ?>">
                <label>Category Name:</label>
                <input type="text" name="category_name" style="margin-bottom: 10px; margin-bottom: 10px;margin-left: 20px;width: 271px;" value="<?php echo esc_attr($category_name); ?>" <?php echo $is_edit ? '' : 'readonly'; ?> required style="margin-bottom: 10px;"><br>

                <div id="car-model-container">
                
                     

                        <label>Car Models:</label>
                        <?php
                        if (!empty($selected_car_models)) {
                            foreach ($selected_car_models as $car_model) {
                                $selected_model_id = $car_model['id'];
                                $position_value = $positions[$selected_model_id] ?? 1;
                                $type_value = $types[$selected_model_id] ?? 1;
                        ?>
                                <div class="car-model-entry" style="display:flex; align-items:center;">
                                    <select name="car_model_id[]"  class="car-model-select" required <?php echo !$is_edit ? 'disabled' : ''; ?> onchange="updateDisabledOptions(this)">
                                        <option value="" disabled>Select Car Model</option>
                                        <?php
                                        // Display the car models in the select dropdown
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
                                    <!-- Sort Field -->
                                    <label style="margin-left:10px;">Sort:</label>
                                    <input type="number" name="sort[]"  required <?php echo !$is_edit ? 'disabled' : ''; ?> style="width: 60px;" min="0" value="<?php echo esc_attr($position_value); ?>"><br>

                                    <label style="margin-left:10px;">Type:</label>
                                    <input type="number" name="type[]"  required <?php echo !$is_edit ? 'disabled' : ''; ?> style="width: 60px;" min="1" value="<?php echo esc_attr($type_value); ?>"><br>

                                    <?php if ($is_edit): ?>
                                        <button type="button" style=" width: 70px; height: 30px; border-radius: 3px; border: 1px solid #9d9d9d; margin-left: 20px;" class="remove-car-model">Delete</button><br><br>
                                    <?php endif; ?>
                                    <br><br>
                                </div>
                            <?php
                            }
                        } else {
                            ?>
                            <div class="car-model-entry" style="display:flex; align-items:center;">
                                <select name="car_model_id[]"  class="car-model-select" required onchange="updateDisabledOptions(this)">
                                    <option value="" disabled selected>Select Car Model</option>
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
                                <input type="number" name="sort[]" required style="width: 60px;" min="0" value=""><br>

                                <label>Position:</label>
                                <input type="number" name="type[]" required style="width: 60px;" min="1" value="1"><br>

                                <button type="button" style=" width: 70px; height: 30px; border-radius: 3px; border: 1px solid #9d9d9d; margin-left: 20px;" class="remove-car-model">Delete</button><br><br>
                            </div>
                        <?php
                        }
                        ?>
                    </div>

                    <?php if ($is_edit): ?>
                        <button type="button" style="width: 70px; height: 30px; margin-bottom: 10px; background: #135E96; color: #ffffff; border: none; border-radius: 3px;" id="add-car-model">Add +</button><br><br>
                    <?php endif; ?>

                    <label>Weight:</label>
                    <input type="number" name="car_model_weight" value="<?php echo esc_attr($weight); ?>" <?php echo $is_edit ? '' : 'readonly'; ?> required style="margin-bottom: 10px;"><br>

                    <label>Status:</label>
                    <label><input type="radio" name="car_model_status" value="1" <?php checked($status, 1); ?> <?php echo $is_edit ? '' : 'disabled'; ?>> Enable</label>
                    <label><input type="radio" name="car_model_status" value="0" <?php checked($status, 0); ?> <?php echo $is_edit ? '' : 'disabled'; ?>> Disabled</label><br><br>

                    <?php if ($is_edit) : ?>
                        <input type="submit" style="margin-bottom: 10px;" value="Update" class="button button-primary">
                    <?php endif; ?>

                    <a href="<?php echo esc_url(admin_url('admin.php?page=recommend-car-model')); ?>" class="button">Back to List</a>
            
            </form>
        </div>
        <script>
            document.getElementById('add-car-model').addEventListener('click', function() {
                const container = document.getElementById('car-model-container');
                const newEntry = container.firstElementChild.cloneNode(true);
                newEntry.querySelector('select[name="car_model_id[]"]').value = '';
                newEntry.querySelector('input[name="sort[]"]').value = '';
                newEntry.querySelector('input[name="type[]"]').value = '';

                const selectedOptions = Array.from(document.querySelectorAll('select[name="car_model_id[]"] option:checked'));
                const allOptions = newEntry.querySelector('select[name="car_model_id[]"]').options;

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

            // Function to add delete functionality to each car model entry
            function addRemoveFunctionality(entry) {
                const removeButton = entry.querySelector('.remove-car-model');
                removeButton.addEventListener('click', function() {
                    const container = document.getElementById('car-model-container');

                    if (container.childElementCount > 1) {
                        const select = entry.querySelector('select[name="car_model_id[]"]');
                        console.log('delete data', select.selectedIndex);
                        const selectedOption = select.options[select.selectedIndex];
                        selectedOption.disabled = false;

                        entry.remove();
                    } else {
                        alert("At least one car model must be present.");
                    }
                });
            }

            document.querySelectorAll('.car-model-entry').forEach(function(entry) {
                addRemoveFunctionality(entry);
            });

            function updateDisabledOptions(selectedDropdown) {
                var allSelects = document.querySelectorAll('.car-model-select');
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

// Update recommend cars
function recommend_handle_update_form_submission()
{
    if (isset($_POST['car_model_id']) && is_array($_POST['car_model_id'])) {
        $recommend_car_models = get_option('recommended_car_models', []);
        $category_name = sanitize_text_field($_POST['category_name']);
        $weight = sanitize_text_field($_POST['car_model_weight']);
        $status = isset($_POST['car_model_status']) ? intval($_POST['car_model_status']) : 0;

        $positions = array_map('sanitize_text_field', $_POST['sort']);
        $types = array_map('sanitize_text_field', $_POST['type']);

        // $normalized_category_name = strtolower(str_replace(' ', '-', $category_name));

        if (array_key_exists($category_name, $recommend_car_models)) {
            $recommend_car_models[$category_name]['weight'] = $weight;
            $recommend_car_models[$category_name]['status'] = $status;

            $car_models = [];
            foreach ($_POST['car_model_id'] as $index => $model_id) {
                $car_models[] = [
                    'id' => intval($model_id),
                    'sort' => intval($positions[$index]),
                    'type' => intval($types[$index])
                ];
            }
            $recommend_car_models[$category_name]['car_models'] = $car_models;
        } else {

            $recommend_car_models[$category_name] = [
                'category' => $category_name,
                'weight' => $weight,
                'status' => $status,
                'car_models' => []
            ];

            foreach ($_POST['car_model_id'] as $index => $model_id) {
                $recommend_car_models[$category_name]['car_models'][] = [
                    'id' => intval($model_id),
                    'sort' => intval($positions[$index]),
                    'type' => intval($types[$index])
                ];
            }
        }

        update_option('recommended_car_models', $recommend_car_models);

        wp_redirect(admin_url('admin.php?page=recommend-car-model'));
        exit;
    } else {
        error_log('No car_model_id found in the POST request.');
    }
}

add_action('admin_post_update_recommend_car_model', 'recommend_handle_update_form_submission');


//delete recommend cars
function recommend_handle_delete_form_submission()
{
    if (isset($_POST['category_name'])) {
        $category_name = sanitize_text_field($_POST['category_name']);
        $recommend_car_models = get_option('recommended_car_models', []);

        // $normalized_category_key = strtolower(str_replace(' ', '-', $category_name));

        if (isset($recommend_car_models[$category_name])) {
            unset($recommend_car_models[$category_name]);
        }

        update_option('recommended_car_models', $recommend_car_models);
    }

    wp_redirect(admin_url('admin.php?page=recommend-car-model'));
    exit;
}

add_action('admin_post_delete_recommend_car_model', 'recommend_handle_delete_form_submission');


function recommend_car_model_admin_menu()
{
    add_menu_page('Recommended Car Models', 'Recommended Car Models', 'manage_options', 'recommend-car-model', 'recommend_car_model_page_callback');
    add_submenu_page('recommend-car-model', 'Add New Car Model', 'Add New Car Model', 'manage_options', 'add-new-recommend-car', 'recommend_car_model_add_page_callback');
    add_submenu_page(null, 'View/Edit Car Model', 'View/Edit Car Model', 'manage_options', 'view-edit-recommend-car', 'recommend_car_model_view_edit_page_callback');
}
add_action('admin_menu', 'recommend_car_model_admin_menu');


//top car model page data
function top_car_model_page_callback()
{
    $top_car_models = get_option('top_car_models', []);
    $grouped_models = [];

    if (!empty($top_car_models)) {
        foreach ($top_car_models as $category_key => $data) {
            if (isset($data['type'], $data['car_models'])) {
                $type = $data['type'];

                if (!isset($grouped_models[$type])) {
                    $grouped_models[$type] = [
                        'models' => [],
                    ];
                }

                foreach ($data['car_models'] as $car_model_data) {
                    $car_model_id = isset($car_model_data['id']) ? $car_model_data['id'] : null;
                    if ($car_model_id) {
                        $car_model = get_post($car_model_id);
                        if ($car_model) {
                            // Append only the car model title
                            $grouped_models[$type]['models'][] = $car_model->post_title;
                        }
                    }
                }
            }
        }
    }
    ?>
    <div class="wrap">
        <h1>Top Car Models</h1>
        <a href="<?php echo admin_url('admin.php?page=add-new-top-car'); ?>" class="button button-primary" style="margin-bottom: 20px;">Add New Car Model</a>
        <table class="wp-list-table widefat fixed striped">
            <thead>
                <tr>
                    <th>Type</th>
                    <th>Car Models</th>
                    <th>Operate</th>
                </tr>
            </thead>
            <tbody>
                <?php
                if (!empty($grouped_models)) {
                    foreach ($grouped_models as $type => $data) {
                        $car_models_list = implode(', ', $data['models']);
                        echo '<tr>';
                        echo '<td>' . esc_html($type) . '</td>';
                        echo '<td>' . esc_html($car_models_list) . '</td>';
                        echo '<td>
                        <div style="display: inline-block; margin-right: 5px;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-top-car&type=' . $type)) . '" class="button">View</a>
                        </div>
                        <div style="display: inline-block;">
                            <a href="' . esc_url(admin_url('admin.php?page=view-edit-top-car&type=' . $type . '&edit=true')) . '" class="button">Edit</a>
                        </div>
                        <div style="display: inline-block; margin-right: 5px;">
                            <form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" onsubmit="return confirmDelete(\'' . esc_js($type) . '\');">
                                <input type="hidden" name="action" value="delete_top_car_model">
                                <input type="hidden" name="type" value="' . esc_attr($type) . '">
                                <button type="submit" class="button button-secondary">Delete</button>
                            </form>
                        </div>
                      </td>';
                        echo '</tr>';
                    }
                } else {
                    echo '<tr><td colspan="5">No top car models added yet.</td></tr>';
                }
                ?>
            </tbody>
        </table>
    </div>
    <script type="text/javascript">
        function confirmDelete(type) {
            return confirm('Are you sure you want to delete the category "' + type + '" and all its car models?');
        }
    </script>
<?php
}

//recommend car add  page
function top_car_model_add_page_callback()
{
?>
    <div class="wrap">
        <h1>Add New Top Car Model</h1>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
            <button type="button" style="width: 80px; height: 30px; color: white; background: #135E96;border: none; border-radius: 3px;" id="add-car-model">Add</button><br><br>
            <input type="hidden" name="action" value="add_new_top_car_model">

            <label>Type:</label>
            <input type="text" name="type" required style="margin-bottom: 10px;margin-left: 49px;width: 273px;"><br>

            <div id="car-model-container">
                <div class="car-model-entry" style="display:flex; align-items:center;">
                    <label>Car Model:</label>
                    <select style="margin-left:20px" name="car_model_id[]" class="car-model-select" required onchange="updateDisabledOptions(this)">
                        <option value="" disabled selected>Select Car Model</option>
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
                    <label style="margin-left: 10px;">Position:</label>
                    <input type="number" name="position[]" required style="width: 60px;" min="1" value="1"><br>

                    <button type="button" style=" height: 30px;border-radius: 3px; border: 1px solid #9d9d9d;margin-left: 14px;" class="remove-car-model">Delete</button><br><br>
                </div>
            </div>



            <!-- Submit -->
            <input type="submit" value="Save Changes" style="margin-top: 30px;" class="button button-primary">
        </form>
    </div>
    <script>
        document.getElementById('add-car-model').addEventListener('click', function() {
            const container = document.getElementById('car-model-container');
            const newEntry = container.firstElementChild.cloneNode(true);

            newEntry.querySelector('select[name="car_model_id[]"]').value = '';
            newEntry.querySelector('input[name="position[]"]').value = '';

            const selectedOptions = Array.from(document.querySelectorAll('select[name="car_model_id[]"] option:checked'));
            const allOptions = newEntry.querySelector('select[name="car_model_id[]"]').options;

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
            const removeButton = entry.querySelector('.remove-car-model');
            removeButton.addEventListener('click', function() {
                const container = document.getElementById('car-model-container');

                if (container.childElementCount > 1) {
                    const select = entry.querySelector('select[name="car_model_id[]"]');
                    const selectedOption = select.options[select.selectedIndex];
                    selectedOption.disabled = false;

                    entry.remove();
                } else {
                    alert("At least one car model must be present.");
                }
            });
        }

        // Add initial remove functionality to the first entry
        addRemoveFunctionality(document.querySelector('.car-model-entry'));

        function updateDisabledOptions(selectedDropdown) {
            var allSelects = document.querySelectorAll('.car-model-select');
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

//add new top cars
function top_handle_add_form_submission()
{
    if (isset($_POST['car_model_id']) && is_array($_POST['car_model_id'])) {
        $top_car_models = get_option('top_car_models', []);
        $type = sanitize_text_field($_POST['type']);
        $positions = $_POST['position'] ?? [];

        $type_key = sanitize_title($type);

        if (!isset($top_car_models[$type_key])) {
            $top_car_models[$type_key] = [
                'type' => $type,
                'car_models' => [],
            ];
        }

        foreach ($_POST['car_model_id'] as $index => $car_model_id) {
            $car_model_id = intval($car_model_id);

            // Store the position of each car model
            $position = isset($positions[$index]) ? intval($positions[$index]) : 1;

            if (!in_array($car_model_id, array_column($top_car_models[$type_key]['car_models'], 'id'))) {
                $top_car_models[$type_key]['car_models'][] = [
                    'id' => $car_model_id,
                    'position' => $position,
                ];
            }
        }

        update_option('top_car_models', $top_car_models);
    }

    wp_redirect(admin_url('admin.php?page=top-car-model'));
    exit;
}
add_action('admin_post_add_new_top_car_model', 'top_handle_add_form_submission');

//top car edit and view page
function top_car_model_view_edit_page_callback()
{
    if (isset($_GET['type'])) {
        $type = isset($_GET['type']) ? sanitize_text_field($_GET['type']) : '';
        $top_car_models = maybe_unserialize(get_option('top_car_models', []));
        $is_edit = isset($_GET['edit']);
        $selected_car_models = [];
        $positions = [];

        if (!empty($top_car_models)) {
            foreach ($top_car_models as $key => $model_data) {
                if ($model_data['type'] === $type) {
                    $selected_car_models = $model_data['car_models'] ?? [];

                    // Extract positions
                    if (!empty($selected_car_models)) {
                        foreach ($selected_car_models as $car_model) {
                            $positions[$car_model['id']] = $car_model['position'];
                        }
                    }
                }
            }
        }
    ?>
        <div class="wrap">
            <h1><?php echo esc_html($is_edit ? 'Edit' : 'View') . ' Car Model'; ?></h1>
            <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
                <input type="hidden" name="action" value="<?php echo esc_attr($is_edit ? 'update_top_car_model' : ''); ?>">

                <label>Type:</label>
                <input type="text" name="type" value="<?php echo esc_attr($type); ?>" <?php echo $is_edit ? '' : 'readonly'; ?> required style="margin-bottom: 10px;"><br>

                <label>Car Models:</label>
                <div id="car-model-container">
                    <?php
                    if (!empty($selected_car_models)) {
                        foreach ($selected_car_models as $car_model) {
                            $selected_model_id = $car_model['id'];
                            $position_value = $positions[$selected_model_id] ?? 1;
                    ?>
                            <div class="car-model-entry"  style="display:flex; align-items:center;">
                                <select name="car_model_id[]" class="car-model-select" required <?php echo !$is_edit ? 'disabled' : ''; ?> onchange="updateDisabledOptions(this)">
                                    <option value="" disabled>Select Car Model</option>
                                    <?php
                                    // Display the car models in the select dropdown
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
                                <label style="margin-left: 15px;">Position:</label>
                                <input type="number" name="position[]" required <?php echo !$is_edit ? 'disabled' : ''; ?> style="width: 60px;" min="1" value="<?php echo esc_attr($position_value); ?>"><br>

                                <?php if ($is_edit): ?>
                                    <button type="button"  style=" height: 30px;border-radius: 3px; border: 1px solid #9d9d9d;margin-left: 14px;" class="remove-car-model">Delete</button><br>
                                <?php endif; ?>
                                <br><br>
                            </div>
                        <?php
                        }
                    } else {
                        ?>
                        <div class="car-model-entry" style="display:flex; align-items:center;">
                            <select name="car_model_id[]" class="car-model-select" required onchange="updateDisabledOptions(this)">
                                <option value="" disabled selected>Select Car Model</option>
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
                            <label style="margin-left: 15px;">Position:</label>
                            <input type="number" name="position[]" required style="width: 60px;" min="1" value="1"><br>

                            <button type="button" class="remove-car-model">Delete</button><br><br>
                        </div>
                    <?php
                    }
                    ?>
                </div>

                <?php if ($is_edit): ?>
                    <button type="button" style="width: 80px; height: 30px; color: white; background: #135E96;border: none; border-radius: 3px;" id="add-car-model">Add +</button>
                <?php endif; ?>

                <?php if ($is_edit) : ?>
                    <input type="submit" style="margin-bottom: 10px;" value="Update" class="button button-primary">
                <?php endif; ?>
            </form>
            <a href="<?php echo esc_url(admin_url('admin.php?page=top-car-model')); ?>" class="button">Back to List</a>
        </div>

        <script>
            document.getElementById('add-car-model').addEventListener('click', function() {
                const container = document.getElementById('car-model-container');
                const newEntry = container.firstElementChild.cloneNode(true);
                newEntry.querySelector('select[name="car_model_id[]"]').value = '';
                newEntry.querySelector('input[name="position[]"]').value = '';

                const selectedOptions = Array.from(document.querySelectorAll('select[name="car_model_id[]"] option:checked'));
                const allOptions = newEntry.querySelector('select[name="car_model_id[]"]').options;

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

            // Function to add delete functionality to each car model entry
            function addRemoveFunctionality(entry) {
                const removeButton = entry.querySelector('.remove-car-model');
                removeButton.addEventListener('click', function() {
                    const container = document.getElementById('car-model-container');

                    if (container.childElementCount > 1) {
                        const select = entry.querySelector('select[name="car_model_id[]"]');
                        console.log('delete data', select.selectedIndex);
                        const selectedOption = select.options[select.selectedIndex];
                        selectedOption.disabled = false;

                        entry.remove();
                    } else {
                        alert("At least one car model must be present.");
                    }
                });
            }

            document.querySelectorAll('.car-model-entry').forEach(function(entry) {
                addRemoveFunctionality(entry);
            });

            function updateDisabledOptions(selectedDropdown) {
                var allSelects = document.querySelectorAll('.car-model-select');
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

// Update top cars
function top_handle_update_form_submission()
{
    if (isset($_POST['car_model_id']) && is_array($_POST['car_model_id'])) {
        $top_car_models = get_option('top_car_models', []);
        $type = sanitize_text_field($_POST['type']);

        $positions = array_map('sanitize_text_field', $_POST['position']);

        $normalized_type = strtolower(str_replace(' ', '-', $type));

        if (array_key_exists($normalized_type, $top_car_models)) {
            $car_models = [];
            foreach ($_POST['car_model_id'] as $index => $model_id) {
                $car_models[] = [
                    'id' => intval($model_id),
                    'position' => intval($positions[$index])
                ];
            }
            $top_car_models[$normalized_type]['car_models'] = $car_models;
        } else {

            $top_car_models[$normalized_type] = [
                'type' => $type,
                'car_models' => []
            ];

            foreach ($_POST['car_model_id'] as $index => $model_id) {
                $top_car_models[$normalized_type]['car_models'][] = [
                    'id' => intval($model_id),
                    'position' => intval($positions[$index])
                ];
            }
        }

        update_option('top_car_models', $top_car_models);

        wp_redirect(admin_url('admin.php?page=top-car-model'));
        exit;
    } else {
        error_log('No car_model_id found in the POST request.');
    }
}

add_action('admin_post_update_top_car_model', 'top_handle_update_form_submission');


//delete top cars
function top_handle_delete_form_submission()
{
    if (isset($_POST['type'])) {
        $type = sanitize_text_field($_POST['type']);
        $top_car_models = get_option('top_car_models', []);

        $normalized_category_key = strtolower(str_replace(' ', '-', $type));

        if (isset($top_car_models[$normalized_category_key])) {
            unset($top_car_models[$normalized_category_key]);
        }

        update_option('top_car_models', $top_car_models);
    }

    wp_redirect(admin_url('admin.php?page=top-car-model'));
    exit;
}

add_action('admin_post_delete_top_car_model', 'top_handle_delete_form_submission');

function top_car_model_admin_menu()
{
    add_menu_page('Top Car Models', 'Top Car Models', 'manage_options', 'top-car-model', 'top_car_model_page_callback');
    add_submenu_page('top-car-model', 'Add New Car Model', 'Add New Car Model', 'manage_options', 'add-new-top-car', 'top_car_model_add_page_callback');
    add_submenu_page(null, 'View/Edit Car Model', 'View/Edit Car Model', 'manage_options', 'view-edit-top-car', 'top_car_model_view_edit_page_callback');
}
add_action('admin_menu', 'top_car_model_admin_menu');

// Ev Car model data
function ev_car_model_page_callback()
{
    $ev_car_models = get_option('ev_car_models', []);
    $listing_posts = get_posts(array(
        'post_type' => 'listing',
        'posts_per_page' => -1,
        'meta_query' => array(
            array(
                'key' => 'is_ev',
                'value' => '1',
                'compare' => '='
            )
        )
    ));
    $listings_by_make = [];
    foreach ($listing_posts as $post) {
        $makes = wp_get_post_terms($post->ID, 'listing_make');
        foreach ($makes as $make) {
            $listings_by_make[$make->name][] = $post;
        }
    }

    ?>
    <div class="wrap">
        <h1>EV Car Model</h1>
        <form method="post" action="<?php echo admin_url('admin-post.php'); ?>" id="ev-car-model-form">
            <input type="hidden" name="action" value="ev_form_submission">
            <input type="hidden" id="delete-ev-car-models" name="delete_ev_car_model[]" value="">

            <h2>Current EV Car Models</h2>
            <ul id="ev-car-model-list">
                <?php if (!empty($ev_car_models)) {
                    foreach ($ev_car_models as $car_model_id) {
                        $car_model = get_post($car_model_id);
                        if ($car_model) {
                            echo '<li data-model-id="' . esc_attr($car_model_id) . '">
                                <span>' . esc_html($car_model->post_title) . '</span>
                                <button type="button" class="delete-ev-model-button" data-ev-delete-id="' . esc_attr($car_model_id) . '">Delete</button>
                            </li>';
                        }
                    }
                } else {
                    echo '<li>No ev car models added yet.</li>';
                } ?>
            </ul>

            <div id="ev-model-container" style="display: flex;flex-direction: column;"> </div>
            <div style="display: flex; align-items: baseline; gap: 13px;">
                <button type="button" id="add-ev-model-button" style="width: 70px; height: 30px; margin-bottom: 10px; background: #135E96; color: #ffffff; border: none; border-radius: 3px;">Add</button>
                <br>
                <input type="submit" class="button button-primary" name="save_ev_car_models" value="Save Changes" style="margin-top: 10px;">
            </div>

        </form>
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const addModelButton = document.getElementById('add-ev-model-button');
            const newModelContainer = document.getElementById('ev-model-container');
            var selectedModels = [];

            addModelButton.addEventListener('click', function() {
                const row = document.createElement('div');
                row.className = 'new-ev-model-row';
                row.style.marginBottom = '15px';

                const select = document.createElement('select');
                select.name = 'new_ev_car_model[]';
                select.style.padding = '5px';
                select.style.marginRight = '10px';

                const placeholderOption = document.createElement('option');
                placeholderOption.value = '';
                placeholderOption.text = 'Select EV Car Models';
                placeholderOption.disabled = true;
                placeholderOption.selected = true;
                select.appendChild(placeholderOption);

                <?php foreach ($listings_by_make as $make_name => $posts) : ?>
                    var optgroup = document.createElement('optgroup');
                    optgroup.label = '<?php echo esc_js($make_name); ?>';
                    <?php foreach ($posts as $post) : ?>
                        var option = document.createElement('option');
                        option.value = '<?php echo esc_attr($post->ID); ?>';
                        option.text = '<?php echo esc_js($post->post_title); ?>';
                        // Disable the option if it's already selected
                        if (selectedModels.includes('<?php echo esc_attr($post->ID); ?>')) {
                            option.disabled = true;
                        }
                        optgroup.appendChild(option);
                    <?php endforeach; ?>
                    select.appendChild(optgroup);
                <?php endforeach; ?>

                row.appendChild(select);
                // Add delete button
                const deleteButton = document.createElement('button');
                deleteButton.type = 'button';
                deleteButton.className = 'delete-new-ev-model-button';
                deleteButton.textContent = 'Delete';
                deleteButton.style.padding = '5px 10px';
                deleteButton.style.marginLeft = '10px';
                row.appendChild(deleteButton);

                newModelContainer.appendChild(row);

                // Update selectedModels when an option is selected
                select.addEventListener('change', function() {
                    if (select.value) {
                        selectedModels.push(select.value);
                        disableSelectedOptions();
                    }
                });

                // Handle delete button click
                deleteButton.addEventListener('click', function() {
                    const selectedValue = select.value;
                    row.remove();
                    // Remove the selected value from selectedModels
                    selectedModels = selectedModels.filter(function(model) {
                        return model !== selectedValue;
                    });
                    disableSelectedOptions();
                });
            });

            // Function to disable selected options in all select elements
            function disableSelectedOptions() {
                const allSelects = document.querySelectorAll('select[name="new_ev_car_model[]"]');
                allSelects.forEach(function(select) {
                    const options = select.querySelectorAll('option');
                    options.forEach(function(option) {
                        if (selectedModels.includes(option.value)) {
                            option.disabled = true;
                        } else {
                            option.disabled = false;
                        }
                    });
                });
            }
            // Handle deletion of existing models
            document.getElementById('ev-car-model-list').addEventListener('click', function(e) {
                if (e.target.classList.contains('delete-ev-model-button')) {
                    const li = e.target.closest('li');
                    const modelId = e.target.getAttribute('data-ev-delete-id');

                    // Create a new hidden input field for each deleted model
                    const deleteInput = document.createElement('input');
                    deleteInput.type = 'hidden';
                    deleteInput.name = 'delete_ev_car_model[]';
                    deleteInput.value = modelId;
                    document.querySelector('form').appendChild(deleteInput);

                    // Remove list item visually
                    li.remove();
                }
            });

            document.getElementById('ev-car-model-form').addEventListener('submit', function(event) {
                const newModels = document.querySelectorAll('select[name="new_ev_car_model[]"]');
                let selectedValues = [];

                newModels.forEach(function(select) {
                    if (select.value) {
                        selectedValues.push(select.value);
                    }
                });

                // Append selected values to the form as hidden inputs
                selectedValues.forEach(function(value) {
                    const hiddenInput = document.createElement('input');
                    hiddenInput.type = 'hidden';
                    hiddenInput.name = 'new_ev_car_model[]';
                    hiddenInput.value = value;
                    event.target.appendChild(hiddenInput);
                });
            });
        });
    </script>
<?php
}

function ev_form_submission()
{
    if (isset($_POST['save_ev_car_models'])) {
        $ev_car_models = get_option('ev_car_models', []);

        // Handle deletion
        if (isset($_POST['delete_ev_car_model']) && is_array($_POST['delete_ev_car_model'])) {
            foreach ($_POST['delete_ev_car_model'] as $model_id) {
                $model_id = intval($model_id);
                $ev_car_models = array_diff($ev_car_models, [$model_id]);
            }
        }

        // Handle new additions
        if (isset($_POST['new_ev_car_model']) && is_array($_POST['new_ev_car_model'])) {
            foreach ($_POST['new_ev_car_model'] as $model_id) {
                $model_id = intval($model_id);
                if (!in_array($model_id, $ev_car_models)) {
                    $ev_car_models[] = $model_id;
                }
            }
        }

        update_option('ev_car_models', $ev_car_models);

        wp_redirect($_SERVER['HTTP_REFERER']);
        exit;
    }
}

add_action('admin_post_ev_form_submission', 'ev_form_submission');
add_action('admin_post_nopriv_ev_form_submission', 'ev_form_submission');
