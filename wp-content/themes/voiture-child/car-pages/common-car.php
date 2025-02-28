<?php
if (!defined('ABSPATH')) {
    exit;
}

// The make, model, and sub page will be passed as variables
$make = isset($make) ? $make : 'unknown';
$model = isset($model) ? $model : 'unknown';
$sub_page = isset($sub_page) ? $sub_page : 'Overview';

$make_term = get_term_by('slug', $make, 'listing_make');
$make_logo_url = get_term_meta($make_term->term_id, 'listing_make_image', true);

$headings = [
    'overview' => ['prefix' => '', 'suffix' => ''],
    'news' => ['prefix' => '', 'suffix' => 'News in Malaysia'],
    'specs' => ['prefix' => '', 'suffix' => 'Specs'],
    'gallery' => ['prefix' => '', 'suffix' => 'Interior & Exterior Images'],
];

ob_start();
?>

<div class="car-header">
    <span><img src="<?php echo $make_logo_url; ?>" alt="Logo" /></span>
    <span>
        <h1 class="car-title"><?php echo $headings[$sub_page]['prefix'] . ' ' . ucfirst($make) . ' ' . ucfirst($model); ?> <?php echo $headings[$sub_page]['suffix']; ?></h1>
    </span>
</div>

<style>
    .car-header {
        display: flex;
        justify-content: left;
        align-items: center;
    }

    .car-header img {
        width: 100px;
        height: 100px;
        border-radius: 50%;
        margin-right: 20px;
    }

    .car-title {
        font-size: 24px;
        font-weight: bold;
        font-style: normal;
    }
</style>

<?php

// Conditional logic to load content based on the subpage
if ($sub_page === 'news') {
    echo do_shortcode('[listing_news make="' . $make . '" model="' . $model . '"]');
} elseif ($sub_page === 'specs') {
    echo do_shortcode('[listing_specs make="' . $make . '" model="' . $model . '"]');
} elseif ($sub_page === 'gallery') {
    echo do_shortcode('[listing_gallery make="' . $make . '" model="' . $model . '"]');
} else {
    echo "<p>Overview for $make $model</p>";
}
?>