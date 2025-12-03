Name:           dnstap2clickhouse
Version:        %{?version}%{?!version:0.0}
Release:        1%{?dist}
Summary:        Read dnstap messages and write them to ClickHouse

License:        GPLv3
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  git golang pandoc gzip
BuildRequires:  systemd-rpm-macros

Provides:       %{name} = %{version}

%description
Read dnstap messages and write them to ClickHouse

%global debug_package %{nil}
%global source_build_dir build
%global man_section 7

%prep
%autosetup

%build
%ifarch x86_64
env CGO_ENABLED=0 go build -v -o %{source_build_dir}/%{name} src/main.go
%else
go build -v -o %{source_build_dir}/%{name} src/main.go
%endif
pandoc --standalone --to man -o %{source_build_dir}/%{name}.%{man_section} doc/%{name}.md
gzip %{source_build_dir}/%{name}.%{man_section}

%install
install -Dpm 0755 %{source_build_dir}/%{name} %{buildroot}%{_bindir}/%{name}
install -Dpm 0644 %{source_build_dir}/%{name}.conf %{buildroot}%{_sysconfdir}/%{name}.conf
install -Dpm 0644 %{source_build_dir}/%{name}.service %{buildroot}%{_unitdir}/%{name}.service
install -Dpm 0644 %{source_build_dir}/%{name}.%{man_section}.gz %{buildroot}%{_mandir}/man%{man_section}/%{name}.%{man_section}.gz

%check
# go test should be here... :)

%post
%systemd_postun_with_restart %{name}.service

%preun
%systemd_preun %{name}.service

%files
%{_bindir}/%{name}
%{_unitdir}/%{name}.service
%config(noreplace) %{_sysconfdir}/%{name}.conf
%{_mandir}/man%{man_section}/%{name}.%{man_section}.gz

%changelog
* Mon Dec 1 2025 Carlos Peon - 0.1-1
- First release%changelog
