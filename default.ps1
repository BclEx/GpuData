properties { 
  $base_dir = resolve-path .
  $build_dir = "$base_dir\build"
  $packageinfo_dir = "$base_dir\nuspecs"
  $release_dir = "$base_dir\Release"
  $sln_file = "$base_dir\GpuData.sln"
  $tools_dir = "$base_dir\tools"
  $version = "1.0.0"
  $config_cpu = "Release"
  $config_cu = "Release.cu"
  $run_tests = $false
}
Framework "4.0"
	
task default -depends Package

task Clean {
	remove-item -force -recurse $build_dir -ErrorAction SilentlyContinue
	remove-item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Clean {
	new-item $build_dir -itemType directory 
	new-item $release_dir -itemType directory 
}

task Compile -depends Init {
	msbuild $sln_file /p:"OutDir=$build_dir\cpu\;Configuration=$config_cpu;LD="
	#msbuild $sln_file /p:"OutDir=$build_dir\cu\;Configuration=$config_cu;LC=20;LD=" /m
	#msbuild $sln_file /target:Rebuild /p:"OutDir=$build_dir\cu\;Configuration=$config_cu;LC=30;LD=" /m
	#msbuild $sln_file /target:Rebuild /p:"OutDir=$build_dir\cu\;Configuration=$config_cu;LC=35;LD=" /m
}

task Test -depends Compile -precondition { return $run_tests } {
	$old = pwd
	cd $build_dir
	#& $tools_dir\xUnit\xunit.console.clr4.exe "$build_dir\11\System.WebEx.Tests.dll" /noshadow
	cd $old
}

task Dependency {
	$package_files = @(Get-ChildItem src -include *packages.config -recurse)
	foreach ($package in $package_files)
	{
		Write-Host $package.FullName
		& $tools_dir\NuGet.exe install $package.FullName -o packages
	}
}

task Package -depends Dependency, Compile, Test {
	$spec_files = @(Get-ChildItem $packageinfo_dir)
	foreach ($spec in $spec_files)
	{
		& $tools_dir\NuGet.exe pack $spec.FullName -o $release_dir -Version $version -Symbols -BasePath $base_dir
	}
}
